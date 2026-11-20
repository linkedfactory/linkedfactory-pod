/*
 * Copyright (c) 2022 Fraunhofer IWU.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.linkedfactory.core.rdf4j.kvin;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.http.KvinHttp;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.util.JsonFormatParser;
import io.github.linkedfactory.core.rdf4j.common.BaseFederatedServiceResolver;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class RemoteServiceTest {
	static final int START_TIME = 1000;

	static final String MOCK_DATA_ITEM1 = """
			{
			    "http://example.org/item1": {
			        "http://example.org/p1": [
			            {
			                "value": 57.934878949512196,
			                "time": 1619424246120
			            }
			        ]
			        }
			}
			""";

	static final String MOCK_DATA_ITEM2 = """
			{
			    "http://example.org/item2": {
			        "http://example.org/p2": [
			            {
			                "value": 57.934878949512196,
			                "time": 1619424246120
			            }
			        ]
			    }
			}
			""";

	File storeDirectory;
	Kvin store;
	Repository repository;

	int kvinHttpCount = 0;
	int kvinHttpFetchCall = 0;

	@Before
	public void init() {
		createStore();
		createRepository();
	}

	void createStore() {
		storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis() + "-" + new Random().nextInt(1000) + "/");
		storeDirectory.deleteOnExit();
		store = new KvinLevelDb(storeDirectory);
	}

	void createRepository() {
		MemoryStore memoryStore = new MemoryStore();
		SailRepository sailRepository = new SailRepository(memoryStore);

		sailRepository.setFederatedServiceResolver(new BaseFederatedServiceResolver() {
			@Override
			public KvinFederatedService createService(String url) {
				Optional<String> endpoint = getKvinServiceUrl(url);
				if (endpoint.isPresent()) {
					KvinFederatedService service = new KvinFederatedService(new KvinHttp(endpoint.get()) {
						@Override
						protected IExtendedIterator<KvinTuple> fetchInternal(List<URI> items, List<URI> properties,
						                                                     URI context, Long end, Long begin,
						                                                     Long limit, Long interval, String op) {
							kvinHttpFetchCall++;
							String response = "";
							if (items.get(0).toString().endsWith("item1")) {
								response = MOCK_DATA_ITEM1;
							} else if (items.get(0).toString().endsWith("item2")) {
								response = MOCK_DATA_ITEM2;
							}
							try {
								JsonFormatParser jsonParser = new JsonFormatParser(
										new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)));
								return jsonParser.parse();
							} catch (IOException e) {
								throw new UncheckedIOException(e);
							}
						}
					}, this::getExecutorService, null, true);
					kvinHttpCount++;
					return service;
				}
				return new KvinFederatedService(store, this::getExecutorService, null, false);
			}
		});

		sailRepository.init();
		this.repository = sailRepository;
	}

	Optional<String> getKvinServiceUrl(String serviceUrl) {
		if (serviceUrl.startsWith("kvin:")) {
			return Optional.of(serviceUrl.replace("kvin:", ""));
		}
		return Optional.empty();
	}

	@Test
	public void basicTest() {
		RepositoryConnection conn = repository.getConnection();

		try {
			int time = START_TIME + 20;

			String values = "values ?item { <item1> }";
			String queryStr = "select * where { " + values + " service <kvin:http://test0.com> { ?item <p1> [ <kvin:to> "
					+ time + " ; <kvin:limit> 1 ; <kvin:value> ?value ; <kvin:time> ?time ] } }";
			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/");
			TupleQueryResult r = query.evaluate();

			while (r.hasNext()) {
				BindingSet bs = r.next();
				Assert.assertEquals("http://example.org/item1", bs.getValue("item").toString());
				Assert.assertEquals("1619424246120", bs.getValue("time").stringValue());
				Assert.assertEquals("57.934878949512196", bs.getValue("value").stringValue());
			}
			r.close();

			Assert.assertEquals(1, kvinHttpCount);
			Assert.assertEquals(1, kvinHttpFetchCall);
		} finally {
			conn.close();
		}
	}

	@Test
	public void multipleEndpointTest() {
		RepositoryConnection conn = repository.getConnection();

		try {
			int time = START_TIME + 20;

			String queryStr = """
					SELECT * {
					  values ?item1 { <item1> }
					  SERVICE <kvin:http://test1.com> {
					    ?item1 <p1> [ <kvin:to> %1$d ; <kvin:limit> 1 ;
					      <kvin:value> ?value ; <kvin:time> ?time
					    ]
					  }
					  SERVICE <kvin:http://test2.com> {
					    <item2> <p2> [ <kvin:to> %1$d ; <kvin:limit> 1 ;
					      <kvin:value> ?value1; <kvin:time> ?time1
					    ]
					  }
					}
					""".formatted(time);

			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/");
			TupleQueryResult r = query.evaluate();

			Assert.assertTrue(r.hasNext());
			while (r.hasNext()) {
				BindingSet bs = r.next();
				if (bs.getValue("time1") != null) {
					Assert.assertEquals("1619424246120", bs.getValue("time").stringValue());
					Assert.assertEquals("57.934878949512196", bs.getValue("value").stringValue());
				}
			}
			Assert.assertEquals(2, kvinHttpCount);
			Assert.assertEquals(2, kvinHttpFetchCall);
			r.close();
		} finally {
			conn.close();
		}
	}

	@Test
	public void aggregationTest() {
		RepositoryConnection conn = repository.getConnection();

		try {
			int time = START_TIME + 20;

			String queryStr = """
					SELECT (SUM(?value) AS ?total) {
					  values ?item { <item1> <item2> }
					  optional {
					    # has only values for item1
					    SERVICE <kvin:http://test1.com> {
					      ?item <p1> [ <kvin:to> %1$d ; <kvin:limit> 1 ; <kvin:value> ?value ]
					    }
					  }
					  optional {
					    # has only values for item2
					    SERVICE <kvin:http://test2.com> {
					      ?item <p2> [ <kvin:to> %1$d ; <kvin:limit> 1 ; <kvin:value> ?value ]
					    }
					  }
					}
					""".formatted(time);

			TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr, "http://example.org/");
			TupleQueryResult r = query.evaluate();

			Assert.assertTrue(r.hasNext());
			while (r.hasNext()) {
				BindingSet bs = r.next();
				Assert.assertEquals(115.869757899024392, ((Literal) bs.getValue("total")).doubleValue(), 10e-6);
			}
			Assert.assertEquals(2, kvinHttpCount);
			r.close();
		} finally {
			conn.close();
		}
	}

	@After
	public void closeRepository() {
		repository.shutDown();
	}
}
