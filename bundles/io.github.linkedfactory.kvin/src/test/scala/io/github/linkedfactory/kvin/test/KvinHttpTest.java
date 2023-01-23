package io.github.linkedfactory.kvin.test;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.Record;
import io.github.linkedfactory.kvin.kvinHttp.KvinHttp;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class KvinHttpTest extends Mockito {

    KvinHttp kvinHttp;
    CloseableHttpClient httpClient;
    CloseableHttpResponse httpResponse;
    HttpPost httpPost;
    HttpGet httpGet;
    StatusLine statusLine;
    HttpEntity entity;

    @Before
    public void setup() {
        this.httpClient = mock(CloseableHttpClient.class);
        this.httpResponse = mock(CloseableHttpResponse.class);
        this.httpPost = mock(HttpPost.class);
        this.httpGet = mock(HttpGet.class);
        this.statusLine = mock(StatusLine.class);
        this.entity = mock(HttpEntity.class);
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
            when(httpResponse.getStatusLine()).thenReturn(statusLine);
            when(statusLine.getStatusCode()).thenReturn(200);
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
            when(httpResponse.getStatusLine()).thenReturn(statusLine);
            when(statusLine.getStatusCode()).thenReturn(200);
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
}
