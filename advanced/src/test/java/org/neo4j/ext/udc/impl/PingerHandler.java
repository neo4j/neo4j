package org.neo4j.ext.udc.impl;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.IOException;

/**
 * 
 */
public class PingerHandler implements HttpRequestHandler {
    public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException {
        System.out.println("got the ping!");
    }
}
