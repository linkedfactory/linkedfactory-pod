/*
 * Copyright (c) 2023 Fraunhofer IWU.
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
package io.github.linkedfactory.core.kvin;

import com.google.inject.Guice;
import io.github.linkedfactory.core.kvin.http.KvinHttp;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.util.KvinTupleGenerator;
import io.github.linkedfactory.service.KvinService;
import io.github.linkedfactory.service.MockHttpServletRequest;
import net.enilink.komma.core.KommaModule;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import net.enilink.komma.model.*;
import net.enilink.platform.lift.util.Globals;
import net.liftweb.common.Box;
import net.liftweb.common.Full;
import net.liftweb.http.*;
import net.liftweb.http.provider.servlet.HTTPRequestServlet;
import net.liftweb.util.VendorJ;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.*;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import scala.Function0;
import scala.PartialFunction;
import scala.collection.immutable.Nil$;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.List;

public class KvinHttpTest extends Mockito {

	static IModelSet modelSet = null;
	Kvin store = null;
	File storeDirectory = null;
	KvinHttp kvinHttp;
	CloseableHttpClient httpClient;
	KvinService kvinService;

	public KvinHttpTest() {
		kvinService = new KvinService(Nil$.MODULE$.$colon$colon("linkedfactory"), new DelegatingKvin(() -> this.store)) {
			@Override
			public Function0<Box<LiftResponse>> apply(Req in) {
				IModelSet ms = Globals.contextModelSet().vend().openOr(null);
				return CurrentReq$.MODULE$.doWith(in, () -> {
					try {
						ms.getUnitOfWork().begin();
						if (isDefinedAt(in)) {
							return super.apply(in);
						} else {
							return (Function0) () -> Box.legacyNullTest((LiftResponse) null);
						}
					} finally {
						ms.getUnitOfWork().end();
					}
				});
			}
		};
	}

	@BeforeClass
	public static void setupClass() {
		// create configuration and a model set factory
		KommaModule module = ModelPlugin.createModelSetModule(ModelPlugin.class.getClassLoader());
		IModelSetFactory factory =
				Guice.createInjector(new ModelSetModule(module))
						.getInstance(IModelSetFactory.class);
		// create a model set with an in-memory repository
		modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"));
		Globals.contextModelSet().theDefault().set(VendorJ.vendor(new Full(modelSet)));
	}

	@AfterClass
	public static void tearDownClass() {
		modelSet.dispose();
		modelSet = null;
	}

	private void createStore() throws IOException {
		storeDirectory = Files.createTempDirectory("leveldb-test-").toFile();
		store = new KvinLevelDb(storeDirectory);
	}

	void deleteDirectory(Path dir) throws IOException {
		Files.walkFileTree(dir, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	CloseableHttpResponse mockedResponse(String json, int statusCode) throws IOException {
		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
		StatusLine statusLine = mock(StatusLine.class);
		when(statusLine.getStatusCode()).thenReturn(statusCode);
		when(response.getStatusLine()).thenReturn(statusLine);
		if (json != null) {
			HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
			when(response.getEntity()).thenReturn(entity);
		}
		return response;
	}

	@Before
	public void setup() throws IOException {
		this.httpClient = mock(CloseableHttpClient.class);
		this.kvinHttp = new KvinHttp("http://example.org/linkedfactory/") {
			@Override
			public CloseableHttpClient getHttpClient() {
				return httpClient;
			}
		};

		createStore();

		Answer<?> answer = invocationOnMock -> {
			HttpRequestBase request = (HttpRequestBase) invocationOnMock.getArguments()[0];
			var mockedRequest = new MockHttpServletRequest(request.getURI().toString()) {
				@Override
				public String method() {
					return request.getMethod();
				}

				@Override
				public String contentType() {
					return "application/json";
				}

				@Override
				public byte[] body() {
					if (request instanceof HttpPost) {
						try {
							return EntityUtils.toString(((HttpPost) request).getEntity()).getBytes();
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
					return null;
				}
			};
			LiftResponse response = kvinService(toReq(mockedRequest)).openOr(() -> null);
			String data = null;
			int code;
			if (response instanceof OutputStreamResponse) {
				var outputStream = new ByteArrayOutputStream();
				((OutputStreamResponse) response).out().apply(outputStream);
				data = outputStream.toString();
				code = response.toResponse().code();
			} else {
				var res = response.toResponse();
				code = res.code();
				if (res instanceof InMemoryResponse && res.size() > 0) {
					data = new String(((InMemoryResponse) res).data(), StandardCharsets.UTF_8);
				}
			}
			return mockedResponse(data, code);
		};
		when(this.httpClient.execute(any())).thenAnswer(answer);
	}

	@After
	public void tearDown() throws IOException {
		store.close();
		store = null;
		deleteDirectory(storeDirectory.toPath());
	}

	private Box<LiftResponse> kvinService(Req request) {
		return kvinService.apply(request).apply();
	}

	private Req toReq(MockHttpServletRequest request) {
		return Req.apply(new HTTPRequestServlet(request, null), Nil$.MODULE$.$colon$colon(PartialFunction.empty()), System.nanoTime());
	}

	@Test
	public void shouldPutMultipleTuples() {
		int numberOfItems = 2;
		int numberOfProperties = 2;
		int numberOfValues = 5;
		var tuples = generateTuples(numberOfItems, numberOfProperties, numberOfValues);
		Assert.assertEquals(numberOfItems * numberOfProperties * numberOfValues, tuples.size());

		// Test the put method with all tuples
		kvinHttp.put(tuples.toArray(new KvinTuple[0]));

		var tuplesSet = new HashSet<>(tuples);

		// iterate over all possible items and retrieve tuples from store
		for (int i = 1; i <= numberOfItems; i++) {
			URI item = URIs.createURI("http://example.org/item" + i);
			var fetched = this.store.fetch(item, null, null, 0).toList();
			Assert.assertEquals(numberOfProperties * numberOfValues, fetched.size());
			for (KvinTuple tuple : fetched) {
				Assert.assertTrue(tuplesSet.remove(tuple));
			}
		}
	}

	@Test
	public void shouldFetchMultipleTuples() {
		int numberOfItems = 2;
		int numberOfProperties = 2;
		int numberOfValues = 5;
		var tuples = generateTuples(numberOfItems, numberOfProperties, numberOfValues);
		Assert.assertEquals(numberOfItems * numberOfProperties * numberOfValues, tuples.size());

		// insert tuples directly into the store
		store.put(tuples.toArray(new KvinTuple[0]));

		var tuplesSet = new HashSet<>(tuples);

		// iterate over all possible items and retrieve tuples via HTTP
		for (int i = 1; i <= numberOfItems; i++) {
			URI item = URIs.createURI("http://example.org/item" + i);
			var fetched = kvinHttp.fetch(item, null, null, 0).toList();
			Assert.assertEquals(numberOfProperties * numberOfValues, fetched.size());
			for (KvinTuple tuple : fetched) {
				Assert.assertTrue(tuplesSet.remove(tuple));
			}
		}
	}

	@Test
	public void shouldReturnPropertiesForItem() {
		int numberOfProperties = 3;
		int numberOfValues = 2;

		// Generate tuples for a single item with multiple properties
		var tuples = generateTuples(1, numberOfProperties, numberOfValues);

		kvinHttp.put(tuples.toArray(new KvinTuple[0]));

		URI item = URIs.createURI("http://example.org/item1");
		var properties = kvinHttp.properties(item, null).toList();
		Assert.assertEquals(numberOfProperties, properties.size());
		for (int i = 1; i <= numberOfProperties; i++) {
			URI expectedProperty = URIs.createURI("http://example.org/property" + i);
			Assert.assertTrue(properties.contains(expectedProperty));
		}
	}

	@Test
	public void shouldThrowOnNon404FetchError() throws Exception {
		doReturn(mockedResponse("{\"code\":\"INTERNAL\"}", 500)).when(httpClient).execute(any());

		KvinException e = Assert.assertThrows(KvinException.class,
				() -> kvinHttp.fetch(URIs.createURI("http://example.org/item1"), null, null, 10).toList());
		Assert.assertTrue(e.getMessage().contains("HTTP 500"));
	}

	@Test
	public void shouldThrowOnNon404PropertiesAndDescendantsError() throws Exception {
		URI item = URIs.createURI("http://example.org/item1");
		doReturn(mockedResponse("", 500)).when(httpClient).execute(any());
		KvinException e = Assert.assertThrows(KvinException.class, () -> kvinHttp.properties(item, null));
		Assert.assertTrue(e.getMessage().contains("HTTP 500"));

		doReturn(mockedResponse("", 503)).when(httpClient).execute(any());
		e = Assert.assertThrows(KvinException.class, () -> kvinHttp.descendants(item, null));
		Assert.assertTrue(e.getMessage().contains("HTTP 503"));
	}

	@Test
	public void shouldThrowKvinExceptionOnConnectionError() throws Exception {
		IOException cause = new IOException("connection refused");
		doThrow(cause).when(httpClient).execute(any());

		URI item = URIs.createURI("http://example.org/item1");
		KvinException e = Assert.assertThrows(KvinException.class,
				() -> kvinHttp.fetch(item, null, null, 10));
		Assert.assertSame(cause, e.getCause());
		Assert.assertThrows(KvinException.class, () -> kvinHttp.properties(item, null));
		Assert.assertThrows(KvinException.class, () -> kvinHttp.descendants(item, null));
		Assert.assertThrows(KvinException.class,
				() -> kvinHttp.put(new KvinTuple(item, URIs.createURI("http://example.org/p"), null, 1, 1)));
	}

	@Test
	public void shouldThrowKvinExceptionOnInvalidFetchResponse() throws Exception {
		doReturn(mockedResponse("{\"http://example.org/item1\": {\"http://example.org/p\": [{\"value\": ",
				200)).when(httpClient).execute(any());

		var values = kvinHttp.fetch(URIs.createURI("http://example.org/item1"), null, null, 10);
		try {
			Assert.assertThrows(KvinException.class, values::toList);
		} finally {
			values.close();
		}
	}

	@Test
	public void shouldThrowOnPutError() throws Exception {
		CloseableHttpResponse response = mockedResponse("{\"code\":\"FORBIDDEN\"}", 403);
		doReturn(response).when(httpClient).execute(any());

		URI item = URIs.createURI("http://example.org/item1");
		KvinException e = Assert.assertThrows(KvinException.class,
				() -> kvinHttp.put(new KvinTuple(item, URIs.createURI("http://example.org/p"), null, 1, 1)));
		Assert.assertTrue(e.getMessage().contains("HTTP 403"));
		verify(response).close();
	}

	@Test
	public void shouldCloseResponses() throws Exception {
		URI item = URIs.createURI("http://example.org/item1");

		CloseableHttpResponse response = mockedResponse("", 200);
		doReturn(response).when(httpClient).execute(any());
		kvinHttp.put(new KvinTuple(item, URIs.createURI("http://example.org/p"), null, 1, 1));
		verify(response).close();

		response = mockedResponse("[{\"@id\": \"http://example.org/p\"}]", 200);
		doReturn(response).when(httpClient).execute(any());
		Assert.assertEquals(List.of(URIs.createURI("http://example.org/p")), kvinHttp.properties(item, null).toList());
		verify(response).close();

		response = mockedResponse("[{\"@id\": \"http://example.org/item1/a\"}]", 200);
		doReturn(response).when(httpClient).execute(any());
		Assert.assertEquals(List.of(URIs.createURI("http://example.org/item1/a")), kvinHttp.descendants(item, null).toList());
		verify(response).close();

		response = mockedResponse("", 500);
		doReturn(response).when(httpClient).execute(any());
		Assert.assertThrows(KvinException.class, () -> kvinHttp.properties(item, null));
		verify(response).close();
	}

	@Test
	public void shouldReturnEmptyOn404Properties() throws Exception {
		doReturn(mockedResponse("", 404)).when(httpClient).execute(any());

		var properties = kvinHttp.properties(URIs.createURI("http://example.org/item1"), null).toList();
		Assert.assertTrue(properties.isEmpty());
	}

	private static List<KvinTuple> generateTuples(int numberOfItems, int numberOfProperties, int numberOfValues) {
		return new KvinTupleGenerator()
				.setStartTime(System.currentTimeMillis())
				// omit float and double values to avoid conversion issues with JSON
				// as it does not differentiate between float and double
				.setDataTypes("int", "long", "string", "boolean", "record", "uri", "array")
				.setItems(numberOfItems)
				.setPropertiesPerItem(numberOfProperties)
				.setValuesPerProperty(numberOfValues)
				.setItemPattern("http://example.org/item{}")
				.setPropertyPattern("http://example.org/property{}")
				.generate()
				.toList();
	}
}
