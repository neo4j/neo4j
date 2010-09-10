package org.neo4j.ext.udc.impl;

import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import org.neo4j.ext.udc.impl.Pinger;
import org.neo4j.ext.udc.impl.PingerHandler;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

/**
 * Unit tests for the UDC statistics pinger.
 */
public class PingerTest {
    private final String EXPTECTED_KERNEL_VERSION = "1.0";
    private final String EXPECTED_STORE_ID = "CAFE";
    private String hostname = "localhost";
    private String serverUrl;

    private LocalTestServer server = null;

    HttpRequestHandler handler = new PingerHandler();

    @Before
    public void setUp() throws Exception {
        server = new LocalTestServer(null, null);
        server.register("/*", handler);
        server.start();

        hostname = server.getServiceHostName();
        serverUrl = "http://" + hostname + ":" + server.getServicePort();
        System.out.println("LocalTestServer available at " + serverUrl);
    }

    /**
     * Test that the LocalTestServer actually works.
     *
     * @throws Exception
     */
    @Test
    public void shouldRespondToHttpClientGet() throws Exception {
        HttpClient httpclient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet(serverUrl + "/?id=storeId+v=kernelVersion");
        System.out.println("HttpClient: http-get " + httpget.getURI());
        HttpResponse response = httpclient.execute(httpget);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            InputStream instream = entity.getContent();
            int l;
            byte[] tmp = new byte[2048];
            while ((l = instream.read(tmp)) != -1) {
            }
        }
        assertThat(response, notNullValue());
        assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
    }


    @Test
    public void shouldPingServer() {
        Pinger p = new Pinger();
        Exception thrownException = null;
        try {
            p.ping(hostname + ":" + server.getServicePort(), EXPECTED_STORE_ID, EXPTECTED_KERNEL_VERSION);
        } catch (IOException e) {
            thrownException = e;
            e.printStackTrace();
        }
        assertThat(thrownException, nullValue());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }
}
