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
import io.github.linkedfactory.core.kvin.util.JsonFormatParser;
import io.github.linkedfactory.service.KvinService;
import io.github.linkedfactory.service.KvinServiceTestBase;
import io.github.linkedfactory.service.MockHttpServletRequest;
import net.enilink.commons.iterator.IExtendedIterator;
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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.*;
import org.mockito.Mockito;
import scala.Function0;
import scala.PartialFunction;
import scala.collection.immutable.Nil$;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

import static org.junit.Assert.*;

public class KvinHttpTest extends Mockito {

    static KvinServiceTestBase base = new KvinServiceTestBase();
    static IModelSet modelSet = null;
    static Kvin store = null;
    static File storeDirectory = null;
    static String item1 = "";
    static String item2 = "";
    KvinHttp kvinHttp;
    CloseableHttpClient httpClient;
    CloseableHttpResponse httpResponse;
    HttpPost httpPost;
    HttpGet httpGet;
    StatusLine statusLine;
    HttpEntity entity;
    KvinService kvinService = null;
    MockHttpServletRequest mockedRequest;


    public KvinHttpTest() {
        kvinService = new KvinService(Nil$.MODULE$.$colon$colon("linkedfactory"), store) {
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
    public static void storeSetup() throws ClassNotFoundException {
        // create configuration and a model set factory
        KommaModule module = ModelPlugin.createModelSetModule(Class.forName("net.enilink.komma.model.ModelPlugin").getClassLoader());
        IModelSetFactory factory = (IModelSetFactory) Guice.createInjector(new ModelSetModule(module)).getInstance(Class.forName("net.enilink.komma.model.IModelSetFactory"));
        // create a model set with an in-memory repository
        modelSet = factory.createModelSet(MODELS.NAMESPACE_URI.appendFragment("MemoryModelSet"));
        Globals.contextModelSet().theDefault().set(VendorJ.vendor(new Full(modelSet)));
        // adding test data
        item1 = base.generateJsonFromSingleTuple();
        item2 = base.generateJsonFromTupleSet();
        createStore();
    }

    @AfterClass
    public static void storeTearDown() throws IOException {
        modelSet.dispose();
        modelSet = null;
        store.close();
        store = null;
        deleteDirectory(storeDirectory.toPath());

    }

    private static void createStore() {
        storeDirectory = new File("/tmp/leveldb-test-" + System.currentTimeMillis() + "-" + new Random().nextInt(1000) + "/");
        storeDirectory.deleteOnExit();
        store = new KvinLevelDb(storeDirectory);
    }

    static void deleteDirectory(Path dir) throws IOException {
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

    @Before
    public void setup() {
        this.httpClient = mock(CloseableHttpClient.class);
        this.httpResponse = mock(CloseableHttpResponse.class);
        this.httpPost = mock(HttpPost.class);
        this.httpGet = mock(HttpGet.class);
        this.statusLine = mock(StatusLine.class);
        this.entity = mock(HttpEntity.class);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
    }

    private Box<LiftResponse> kvinRest(Req request) {
        return kvinService.apply(request).apply();
    }

    private Req toReq(MockHttpServletRequest request) {
        return Req.apply(new HTTPRequestServlet(request, null), Nil$.MODULE$.$colon$colon(PartialFunction.empty()), System.nanoTime());
    }

    @Test
    public void shouldDoSimplePut() {
        try {
            URI item = URIs.createURI("http://dm.adaproq.de/vocab/gtc");
            URI property = URIs.createURI("http://dm.adaproq.de/vocab/workpiece");
            Record value = new Record(URIs.createURI("http://dm.adaproq.de/vocab/record"), new Object() {
                public final String msg = "Error1";
                public final int nr = 1;

            });
            long time = 1653292320;
            int seqNr = 1;
            // mocking settings for http client response
            when(httpClient.execute(httpPost)).thenReturn(this.httpResponse);
            this.kvinHttp = new KvinHttp("http://samplehost.de") {
                @Override
                public HttpPost createHttpPost(String endpoint) {
                    return httpPost;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return httpClient;
                }
            };
            // test tuples
            KvinTuple tuple = new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, time, seqNr, value);
            KvinTuple tuple1 = new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, time, seqNr, new Object() {
                public final String id = "http://dm.adaproq.de/vocab/wp1";
            });
            kvinHttp.put(tuple, tuple1);
            // testing if response code is 200 OK
            assertEquals(this.httpClient.execute(this.httpPost).getStatusLine().getStatusCode(), 200);
        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp put() method");
        }
    }

    @Test
    public void shouldDoBatchPut() {
        try {
            URI item = URIs.createURI("http://dm.adaproq.de/vocab/gtc");
            URI item1 = URIs.createURI("http://dm.adaproq.de/vocab/wp1");
            URI property = URIs.createURI("http://dm.adaproq.de/vocab/workpiece");
            URI property1 = URIs.createURI("http://dm.adaproq.de/vocab/abschnitt");
            URI property2 = URIs.createURI("http://dm.adaproq.de/vocab/origin");
            String value = "test";
            String value1 = "anfang";
            Object value2 = new Object() {
                public final String id = "http://dm.adaproq.de/vocab/coil-7220578838";
            };
            long time = 1653292320;
            int seqNr = 1;
            // mocking settings for http client response
            when(httpClient.execute(httpPost)).thenReturn(this.httpResponse);
            this.kvinHttp = new KvinHttp("http://samplehost.de") {
                @Override
                public HttpPost createHttpPost(String endpoint) {
                    return httpPost;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return httpClient;
                }
            };
            // test tuples
            KvinTuple tuple = new KvinTuple(item, property, Kvin.DEFAULT_CONTEXT, time, seqNr, new Object() {
                public final String id = "http://dm.adaproq.de/vocab/wp1";
            });
            KvinTuple tuple1 = new KvinTuple(item1, property1, Kvin.DEFAULT_CONTEXT, time, seqNr, value1);
            KvinTuple tuple2 = new KvinTuple(item1, property2, Kvin.DEFAULT_CONTEXT, time, seqNr, value2);
            kvinHttp.put(new ArrayList<>(Arrays.asList(tuple, tuple1, tuple2)));
            // testing if response code is 200 OK
            assertEquals(this.httpClient.execute(this.httpPost).getStatusLine().getStatusCode(), 200);
        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp put() method");
        }
    }

    @Test
    public void shouldDoFetch() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/machine1/sensor1");
            URI property = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/value");
            long limit = 0;
            // mocking settings for http client response
            when(httpResponse.getEntity()).thenReturn(new StringEntity(getResourceFileContent("KvinHttpFetchMethodContent.json")));
            when(httpClient.execute(httpGet)).thenReturn(this.httpResponse);
            this.kvinHttp = new KvinHttp("http://samplehost.de") {
                @Override
                public HttpGet createHttpGet(String endpoint) {
                    return httpGet;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return httpClient;
                }
            };
            // kvinHttp.fetch(item, property, null, 1619424246100L, 1619424246100L, 0, 1000, "avg");
            IExtendedIterator<KvinTuple> tuples = kvinHttp.fetch(item, property, null, limit);
            assertNotNull(tuples);
            int index = 0;
            while (tuples.hasNext()) {
                KvinTuple t = tuples.next();
                if (index == 2 || index == 5) {
                    assertTrue(t.value instanceof Record);
                }
                index++;
            }
            assertEquals(index, 6);


        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp fetch() method");
        }
    }

    @Test
    public void shouldFetchDescendants() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/machine1");
            // mocking settings for http client response
            when(this.httpResponse.getEntity()).thenReturn(new StringEntity(getResourceFileContent("KvinHttpDescendantsMethodContent.json")));
            when(httpClient.execute(httpGet)).thenReturn(this.httpResponse);
            this.kvinHttp = new KvinHttp("http://samplehost.de") {
                @Override
                public HttpGet createHttpGet(String endpoint) {
                    return httpGet;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return httpClient;
                }
            };
            IExtendedIterator<URI> descendants = kvinHttp.descendants(item);
            assertNotNull(descendants);
            int descendantCount = 0;
            while (descendants.hasNext()) {
                URI descendant = descendants.next();
                assertTrue(descendant instanceof URI);
                descendantCount++;
            }
            assertEquals(descendantCount, 10);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp descendants() method");
        }
    }

    @Test
    public void shouldFetchProperties() {
        try {
            URI item = URIs.createURI("http://localhost:8080/linkedfactory/demofactory/machine1/sensor1");
            // mocking settings for http client response
            when(this.httpResponse.getEntity()).thenReturn(new StringEntity(getResourceFileContent("KvinHttpPropertiesMethodContent.json")));
            when(httpClient.execute(httpGet)).thenReturn(this.httpResponse);
            this.kvinHttp = new KvinHttp("http://samplehost.de") {
                @Override
                public HttpGet createHttpGet(String endpoint) {
                    return httpGet;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return httpClient;
                }
            };
            IExtendedIterator<URI> properties = kvinHttp.properties(item);
            assertNotNull(properties);
            int propertyCount = 0;
            while (properties.hasNext()) {
                URI property = properties.next();
                assertTrue(property instanceof URI);
                propertyCount++;
            }
            assertEquals(propertyCount, 4);

        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp properties() method");
        }
    }

    private String getResourceFileContent(String filename) throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(filename);
        InputStreamReader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader);
        StringBuilder stringBuilder = new StringBuilder();
        reader.lines().forEach((line -> {
            stringBuilder.append(line);
        }));
        return stringBuilder.toString();
    }

    @Test
    public void shouldDoPutToKvinService() {
        try {
            // delegating request to kvinservice
            when(httpResponse.getEntity()).thenAnswer(invocationOnMock -> {
                var req = toReq(mockedRequest);
                var response = kvinRest(req).map(LiftResponse::toResponse).openOr(null);
                if (response instanceof OutputStreamResponse) {
                    assertEquals(((OutputStreamResponse) response).code(), 200);
                    var outputStream = new ByteArrayOutputStream();
                    outputStream = (ByteArrayOutputStream) ((OutputStreamResponse) response).out();
                    return new StringEntity(outputStream.toString());
                } else if (response instanceof InMemoryResponse) {
                    assertEquals(((InMemoryResponse) response).code(), 200);
                    return new StringEntity(Arrays.toString(((InMemoryResponse) response).data()));
                } else {
                    throw new RuntimeException("Invalid response type");
                }

            });
            CloseableHttpClient client = spy(HttpClients.createDefault());
            HttpPost postRequest = spy(new HttpPost("http://foo.com/linkedfactory/values"));
            StringEntity requestPayload = new StringEntity(
                    item2,
                    ContentType.APPLICATION_JSON
            );
            postRequest.setEntity(requestPayload);
            when(client.execute(postRequest)).thenAnswer(invocationOnMock -> {
                HttpPost request = (HttpPost) invocationOnMock.getArguments()[0];
                mockedRequest = new MockHttpServletRequest(request.getURI().toString()) {
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
                        try {
                            return EntityUtils.toString(request.getEntity()).getBytes();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                return httpResponse;
            });
            this.kvinHttp = new KvinHttp("http://foo.com/linkedfactory/values") {
                @Override
                public HttpPost createHttpPost(String endpoint) {
                    return postRequest;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return client;
                }
            };
            kvinHttp.put(new JsonFormatParser(new ByteArrayInputStream(item2.getBytes())).parse());

        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp shouldDoPutToKvinService() method");
        }
    }

    @Test
    public void shouldDoFetchFromKvinService() {
        try {
            URI item = URIs.createURI("http://example.org/item2");
            URI property = URIs.createURI("http://example.org/properties/p2");
            long limit = 0;
            // delegating request to kvinservice
            when(httpResponse.getEntity()).thenAnswer(invocationOnMock -> {
                var req = toReq(mockedRequest);
                var response = kvinRest(req).map(LiftResponse::toResponse).openOr(null);
                if (response instanceof OutputStreamResponse) {
                    assertEquals(((OutputStreamResponse) response).code(), 200);
                    var outputStream = new ByteArrayOutputStream();
                    ((OutputStreamResponse) response).out().apply(outputStream);
                    return new StringEntity(outputStream.toString());
                } else if (response instanceof InMemoryResponse) {
                    assertEquals(((InMemoryResponse) response).code(), 200);
                    return new StringEntity(Arrays.toString(((InMemoryResponse) response).data()));
                } else {
                    throw new RuntimeException("Invalid response type");
                }
            });
            // spying client and request
            CloseableHttpClient client = spy(HttpClients.createDefault());
            URIBuilder uriBuilder = new URIBuilder("http://foo.com/linkedfactory/values");
            uriBuilder.setParameter("item", item.toString());
            uriBuilder.setParameter("property", property.toString());
            HttpGet getRequest = spy(new HttpGet(uriBuilder.build().toString()));
            when(client.execute(getRequest)).thenAnswer(invocationOnMock -> {
                HttpGet request = (HttpGet) invocationOnMock.getArguments()[0];
                mockedRequest = new MockHttpServletRequest(request.getURI().toString()) {
                    @Override
                    public String method() {
                        return request.getMethod();
                    }
                };
                return httpResponse;
            });
            // actual kvinHttp request
            this.kvinHttp = new KvinHttp("http://foo.com/linkedfactory/values") {
                @Override
                public HttpGet createHttpGet(String endpoint) {
                    return getRequest;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return client;
                }
            };
            IExtendedIterator<KvinTuple> tuples = kvinHttp.fetch(item, property, null, limit);
            assertNotNull(tuples);
            int count = 0;
            while (tuples.hasNext()) {
                KvinTuple tuple = tuples.next();
                if (count == 0) {
                    assertEquals(tuple.item.toString(), "http://example.org/item2");
                    assertEquals(tuple.property.toString(), "http://example.org/properties/p2");
                    assertEquals(tuple.time, 1675324136198L);
                    assertEquals(tuple.value.toString(), "6.333333333333333");
                }
                count++;
            }
            assertEquals(count, 6);
        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp shouldDoFetchFromKvinService() method");
        }
    }

    @Test
    public void shouldDoFetchWithLimitFromKvinService() {
        try {
            URI item = URIs.createURI("http://example.org/item2");
            URI property = URIs.createURI("http://example.org/properties/p2");
            long limit = 2;
            // delegating request to kvinservice
            when(httpResponse.getEntity()).thenAnswer(invocationOnMock -> {
                var req = toReq(mockedRequest);
                var response = kvinRest(req).map(LiftResponse::toResponse).openOr(null);
                if (response instanceof OutputStreamResponse) {
                    assertEquals(((OutputStreamResponse) response).code(), 200);
                    var outputStream = new ByteArrayOutputStream();
                    ((OutputStreamResponse) response).out().apply(outputStream);
                    return new StringEntity(outputStream.toString());
                } else if (response instanceof InMemoryResponse) {
                    assertEquals(((InMemoryResponse) response).code(), 200);
                    return new StringEntity(Arrays.toString(((InMemoryResponse) response).data()));
                } else {
                    throw new RuntimeException("Invalid response type");
                }
            });
            // spying client and request
            CloseableHttpClient client = spy(HttpClients.createDefault());
            URIBuilder uriBuilder = new URIBuilder("http://foo.com/linkedfactory/values");
            uriBuilder.setParameter("item", item.toString());
            uriBuilder.setParameter("property", property.toString());
            uriBuilder.setParameter("limit", Long.toString(limit));
            HttpGet getRequest = spy(new HttpGet(uriBuilder.build().toString()));
            when(client.execute(getRequest)).thenAnswer(invocationOnMock -> {
                HttpGet request = (HttpGet) invocationOnMock.getArguments()[0];
                mockedRequest = new MockHttpServletRequest(request.getURI().toString()) {
                    @Override
                    public String method() {
                        return request.getMethod();
                    }
                };
                return httpResponse;
            });
            // actual kvinHttp request
            this.kvinHttp = new KvinHttp("http://foo.com/linkedfactory/values") {
                @Override
                public HttpGet createHttpGet(String endpoint) {
                    return getRequest;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return client;
                }
            };
            IExtendedIterator<KvinTuple> tuples = kvinHttp.fetch(item, property, null, limit);
            assertNotNull(tuples);
            assertEquals(tuples.toList().size(), 2);
        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp shouldDoFetchFromKvinService() method");
        }
    }

    @Test
    public void shouldDoFetchWithOpFromKvinService() {
        try {
            URI item = URIs.createURI("http://example.org/item2");
            URI property = URIs.createURI("http://example.org/properties/p2");
            long limit = 0;
            String op = "sum";
            int interval = 10000;
            // delegating request to kvinservice
            when(httpResponse.getEntity()).thenAnswer(invocationOnMock -> {
                var req = toReq(mockedRequest);
                var response = kvinRest(req).map(LiftResponse::toResponse).openOr(null);
                if (response instanceof OutputStreamResponse) {
                    assertEquals(((OutputStreamResponse) response).code(), 200);
                    var outputStream = new ByteArrayOutputStream();
                    ((OutputStreamResponse) response).out().apply(outputStream);
                    return new StringEntity(outputStream.toString());
                } else if (response instanceof InMemoryResponse) {
                    assertEquals(((InMemoryResponse) response).code(), 200);
                    return new StringEntity(Arrays.toString(((InMemoryResponse) response).data()));
                } else {
                    throw new RuntimeException("Invalid response type");
                }
            });
            // spying client and request
            CloseableHttpClient client = spy(HttpClients.createDefault());
            URIBuilder uriBuilder = new URIBuilder("http://foo.com/linkedfactory/values");
            uriBuilder.setParameter("item", item.toString());
            uriBuilder.setParameter("property", property.toString());
            uriBuilder.setParameter("limit", Long.toString(limit));
            uriBuilder.setParameter("op", op);
            uriBuilder.setParameter("interval", Long.toString(interval));
            HttpGet getRequest = spy(new HttpGet(uriBuilder.build().toString()));
            when(client.execute(getRequest)).thenAnswer(invocationOnMock -> {
                HttpGet request = (HttpGet) invocationOnMock.getArguments()[0];
                mockedRequest = new MockHttpServletRequest(request.getURI().toString()) {
                    @Override
                    public String method() {
                        return request.getMethod();
                    }
                };
                return httpResponse;
            });
            // actual kvinHttp request
            this.kvinHttp = new KvinHttp("http://foo.com/linkedfactory/values") {
                @Override
                public HttpGet createHttpGet(String endpoint) {
                    return getRequest;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return client;
                }
            };
            IExtendedIterator<KvinTuple> tuples = kvinHttp.fetch(item, property, null, 1675324136198L, 1675324121191L, limit, 10000, "sum");
            assertNotNull(tuples);
            int count = 0;
            while (tuples.hasNext()) {
                KvinTuple tuple = tuples.next();
                if (count == 0) {
                    assertEquals(tuple.value.toString(), "18.916666666666664");
                } else {
                    assertEquals(tuple.value.toString(), "18.533333333333335");
                }
                count++;
            }
            assertEquals(count, 2);
        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp shouldDoFetchFromKvinService() method");
        }
    }

    @Test
    public void shouldDoFetchPropertiesFromKvinService() {
        try {
            URI item = URIs.createURI("http://example.org/item2");
            // delegating request to kvinservice
            when(httpResponse.getEntity()).thenAnswer(invocationOnMock -> {
                var req = toReq(mockedRequest);
                var response = kvinRest(req).map(LiftResponse::toResponse).openOr(null);
                if (response instanceof OutputStreamResponse) {
                    assertEquals(((OutputStreamResponse) response).code(), 200);
                    var outputStream = new ByteArrayOutputStream();
                    ((OutputStreamResponse) response).out().apply(outputStream);
                    return new StringEntity(outputStream.toString());
                } else if (response instanceof InMemoryResponse) {
                    assertEquals(((InMemoryResponse) response).code(), 200);
                    return new StringEntity(new String(((InMemoryResponse) response).data()));
                } else {
                    throw new RuntimeException("Invalid response type");
                }
            });
            // spying client and request
            CloseableHttpClient client = spy(HttpClients.createDefault());
            URIBuilder uriBuilder = new URIBuilder("http://foo.com/linkedfactory/properties");
            uriBuilder.setParameter("item", item.toString());
            HttpGet getRequest = spy(new HttpGet(uriBuilder.build().toString()));
            when(client.execute(getRequest)).thenAnswer(invocationOnMock -> {
                HttpGet request = (HttpGet) invocationOnMock.getArguments()[0];
                mockedRequest = new MockHttpServletRequest(request.getURI().toString()) {
                    @Override
                    public String method() {
                        return request.getMethod();
                    }
                };
                return httpResponse;
            });
            // actual kvinHttp request
            this.kvinHttp = new KvinHttp("http://foo.com/linkedfactory/properties") {
                @Override
                public HttpGet createHttpGet(String endpoint) {
                    return getRequest;
                }

                @Override
                public CloseableHttpClient getHttpClient() {
                    return client;
                }
            };
            IExtendedIterator<URI> uris = kvinHttp.properties(item);
            assertNotNull(uris);
            int count = 0;
            while (uris.hasNext()) {
                URI uri = uris.next();
                assertEquals(uri.toString(), "http://example.org/properties/p2");
                count++;
            }
            assertEquals(count, 1);
        } catch (Exception e) {
            fail("Something went wrong while testing KvinHttp shouldDoFetchFromKvinService() method");
        }
    }
}
