/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriInfo;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import org.neo4j.server.rest.batch.BatchOperations;
import org.neo4j.server.rest.batch.StreamingBatchOperationResults;
import org.neo4j.server.rest.domain.BatchOperationFailedException;
import org.neo4j.server.rest.repr.formats.StreamingJsonFormat;
import org.neo4j.server.web.WebServer;

public class StreamingBatchOperations extends BatchOperations
{

    private static final Logger LOGGER = Log.getLogger(StreamingBatchOperations.class);
    private StreamingBatchOperationResults results;

    public StreamingBatchOperations( WebServer webServer )
    {
        super( webServer );
    }

    public void readAndExecuteOperations( UriInfo uriInfo, HttpHeaders httpHeaders, HttpServletRequest req,
                                          InputStream body, ServletOutputStream output ) throws IOException, ServletException {
        results = new StreamingBatchOperationResults(jsonFactory.createJsonGenerator(output),output);
        Map<Integer, String> locations = results.getLocations();
        parseAndPerform( uriInfo, httpHeaders, req, body, locations );
        results.close();
    }
/*
[{"from":"/node","body":{"extensions":{},"paged_traverse":"http://localhost:7474/db/data/node/1/paged/traverse/{returnType}{?pageSize,leaseTime}","outgoing_relationships":"http://localhost:7474/db/data/node/1/relationships/out","traverse":"http://localhost:7474/db/data/node/1/traverse/{returnType}","all_typed_relationships":"http://localhost:7474/db/data/node/1/relationships/all/{-list|&|types}","property":"http://localhost:7474/db/data/node/1/properties/{key}","all_relationships":"http://localhost:7474/db/data/node/1/relationships/all","self":"http://localhost:7474/db/data/node/1","properties":"http://localhost:7474/db/data/node/1/properties","outgoing_typed_relationships":"http://localhost:7474/db/data/node/1/relationships/out/{-list|&|types}","incoming_relationships":"http://localhost:7474/db/data/node/1/relationships/in","incoming_typed_relationships":"http://localhost:7474/db/data/node/1/relationships/in/{-list|&|types}","create_relationship":"http://localhost:7474/db/data/node/1/relationships","data":{"age":"1"}},"location":"http://localhost:7474/db/data/node/1","status":201},{"from":"/node","body":{"message":"java.util.ArrayList cannot be cast to java.util.Map","exception":"org.neo4j.server.rest.repr.BadInputException: java.util.ArrayList cannot be cast to java.util.Map","stacktrace":["org.neo4j.server.rest.repr.formats.JsonFormat.readMap(JsonFormat.java:92)","org.neo4j.server.rest.web.RestfulGraphDatabase.createNode(RestfulGraphDatabase.java:195)","sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)","sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:39)","sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)","java.lang.reflect.Method.invoke(Method.java:597)","com.sun.jersey.spi.container.JavaMethodInvokerFactory$1.invoke(JavaMethodInvokerFactory.java:60)","com.sun.jersey.server.impl.model.method.dispatch.AbstractResourceMethodDispatchProvider$ResponseOutInvoker._dispatch(AbstractResourceMethodDispatchProvider.java:205)","com.sun.jersey.server.impl.model.method.dispatch.ResourceJavaMethodDispatcher.dispatch(ResourceJavaMethodDispatcher.java:75)","com.sun.jersey.server.impl.uri.rules.HttpMethodRule.accept(HttpMethodRule.java:288)","com.sun.jersey.server.impl.uri.rules.RightHandPathRule.accept(RightHandPathRule.java:147)","com.sun.jersey.server.impl.uri.rules.ResourceClassRule.accept(ResourceClassRule.java:108)","com.sun.jersey.server.impl.uri.rules.RightHandPathRule.accept(RightHandPathRule.java:147)","com.sun.jersey.server.impl.uri.rules.RootResourceClassesRule.accept(RootResourceClassesRule.java:84)","com.sun.jersey.server.impl.application.WebApplicationImpl._handleRequest(WebApplicationImpl.java:1469)","com.sun.jersey.server.impl.application.WebApplicationImpl._handleRequest(WebApplicationImpl.java:1400)","com.sun.jersey.server.impl.application.WebApplicationImpl.handleRequest(WebApplicationImpl.java:1349)","com.sun.jersey.server.impl.application.WebApplicationImpl.handleRequest(WebApplicationImpl.java:1339)","com.sun.jersey.spi.container.servlet.WebComponent.service(WebComponent.java:416)","com.sun.jersey.spi.container.servlet.ServletContainer.service(ServletContainer.java:537)","com.sun.jersey.spi.container.servlet.ServletContainer.service(ServletContainer.java:699)","javax.servlet.http.HttpServlet.service(HttpServlet.java:820)","org.mortbay.jetty.servlet.ServletHolder.handle(ServletHolder.java:511)","org.mortbay.jetty.servlet.ServletHandler.handle(ServletHandler.java:390)","org.mortbay.jetty.servlet.SessionHandler.handle(SessionHandler.java:182)","org.mortbay.jetty.handler.ContextHandler.handle(ContextHandler.java:765)","org.mortbay.jetty.handler.HandlerCollection.handle(HandlerCollection.java:114)","org.mortbay.jetty.handler.HandlerWrapper.handle(HandlerWrapper.java:152)","org.neo4j.server.web.Jetty6WebServer.invokeDirectly(Jetty6WebServer.java:201)","org.neo4j.server.rest.web.StreamingBatchOperations.invoke(StreamingBatchOperations.java:62)","org.neo4j.server.rest.batch.BatchOperations.performRequest(BatchOperations.java:179)","org.neo4j.server.rest.batch.BatchOperations.parseAndPerform(BatchOperations.java:150)","org.neo4j.server.rest.web.StreamingBatchOperations.readAndExecuteOperations(StreamingBatchOperations.java:52)","org.neo4j.server.rest.web.BatchOperationService$1.write(BatchOperationService.java:88)","com.sun.jersey.core.impl.provider.entity.StreamingOutputProvider.writeTo(StreamingOutputProvider.java:71)","com.sun.jersey.core.impl.provider.entity.StreamingOutputProvider.writeTo(StreamingOutputProvider.java:57)","com.sun.jersey.spi.container.ContainerResponse.write(ContainerResponse.java:306)","com.sun.jersey.server.impl.application.WebApplicationImpl._handleRequest(WebApplicationImpl.java:1437)","com.sun.jersey.server.impl.application.WebApplicationImpl.handleRequest(WebApplicationImpl.java:1349)","com.sun.jersey.server.impl.application.WebApplicationImpl.handleRequest(WebApplicationImpl.java:1339)","com.sun.jersey.spi.container.servlet.WebComponent.service(WebComponent.java:416)","com.sun.jersey.spi.container.servlet.ServletContainer.service(ServletContainer.java:537)","com.sun.jersey.spi.container.servlet.ServletContainer.service(ServletContainer.java:699)","javax.servlet.http.HttpServlet.service(HttpServlet.java:820)","org.mortbay.jetty.servlet.ServletHolder.handle(ServletHolder.java:511)","org.mortbay.jetty.servlet.ServletHandler.handle(ServletHandler.java:390)","org.mortbay.jetty.servlet.SessionHandler.handle(SessionHandler.java:182)","org.mortbay.jetty.handler.ContextHandler.handle(ContextHandler.java:765)","org.mortbay.jetty.handler.HandlerCollection.handle(HandlerCollection.java:114)","org.mortbay.jetty.handler.HandlerWrapper.handle(HandlerWrapper.java:152)","org.mortbay.jetty.Server.handle(Server.java:322)","org.mortbay.jetty.HttpConnection.handleRequest(HttpConnection.java:542)","org.mortbay.jetty.HttpConnection$RequestHandler.content(HttpConnection.java:943)","org.mortbay.jetty.HttpParser.parseNext(HttpParser.java:756)","org.mortbay.jetty.HttpParser.parseAvailable(HttpParser.java:218)","org.mortbay.jetty.HttpConnection.handle(HttpConnection.java:404)","org.mortbay.io.nio.SelectChannelEndPoint.run(SelectChannelEndPoint.java:410)","org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool.java:582)"]}null,"status":400}]
 */
    @Override
    protected void invoke(String method,  String path, String body, Integer id, URI targetUri, InternalJettyServletRequest req, InternalJettyServletResponse res ) throws IOException, ServletException
    {
        results.startOperation(path,id);
        try {
            res = new BatchInternalJettyServletResponse(results.getServletOutputStream());
            webServer.invokeDirectly(targetUri.getPath(), req, res);
        } catch(Exception e) {
            LOGGER.warn( e );
            results.writeError( 500, e.getMessage() );
            throw new BatchOperationFailedException(500, e.getMessage(),e );

        }
        final int status = res.getStatus();
        if (is2XXStatusCode(status))
        {
            results.addOperationResult(status,id,res.getHeader("Location"));
        }
        else
        {
            final String message = "Error " + status + " executing batch operation: " + ((id!=null) ? id + ". ":"") + method + " " + path + " " + body;
            results.writeError( status, res.getReason() );
            throw new BatchOperationFailedException( status, message, new Exception( res.getReason() ) );
        }
    }

    protected void addHeaders(final InternalJettyServletRequest res,
            final HttpHeaders httpHeaders)
    {
        super.addHeaders( res,httpHeaders );
        res.addHeader(StreamingJsonFormat.STREAM_HEADER,"true");
    }

    private static class BatchInternalJettyServletResponse extends InternalJettyServletResponse {
        private final ServletOutputStream output;

        public BatchInternalJettyServletResponse(ServletOutputStream output) {
            this.output = output;
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return output;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            return new PrintWriter( new OutputStreamWriter( output, "UTF-8") );
        }
    }
}
