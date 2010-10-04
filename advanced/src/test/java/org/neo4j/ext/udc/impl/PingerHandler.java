package org.neo4j.ext.udc.impl;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class PingerHandler implements HttpRequestHandler {

    Map<String, String> queryMap = new HashMap<String, String>();

    public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException {
        System.out.println("got the ping, as: " + httpRequest.getRequestLine().getUri());

        final String requestUri = httpRequest.getRequestLine().getUri();
        final String[] query = requestUri.split("\\?");
        if (query.length > 1) {
            String[] params = query[1].split("\\+");
            if (params.length > 0) {
                for (String param : params) {
                    String[] pair = param.split("=");
                    String key = URLDecoder.decode(pair[0], "UTF-8");
                    String value = URLDecoder.decode(pair[1], "UTF-8");
                    System.out.println("\t:" + key + " = " + value);
                    queryMap.put(key, value);
                }
            }
            else
            {
                System.out.println("no params found in query");
            }
        }
        else
        {
            System.out.println("no query string found");
        }
    }

    public Map<String, String> getQueryMap()
    {
            return queryMap;
    }

}
