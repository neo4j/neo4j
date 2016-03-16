/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.web;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.component.AbstractLifeCycle;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.concurrent.AsyncEvents;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;
import org.neo4j.logging.async.AsyncLogEvent;
import org.neo4j.logging.async.AsyncLogProvider;

public class AsyncRequestLog
        extends AbstractLifeCycle
        implements RequestLog, Consumer<AsyncLogEvent>, AsyncEvents.Monitor
{
    private final Log log;
    private final ExecutorService asyncLogProcessingExecutor;
    private final AsyncEvents<AsyncLogEvent> asyncEventProcessor;

    public AsyncRequestLog( FileSystemAbstraction fs, String logFile, long rotationSize, int rotationKeepNumber ) throws IOException
    {
        NamedThreadFactory threadFactory = new NamedThreadFactory( "HTTP-Log-Rotator", true );
        ExecutorService rotationExecutor = Executors.newCachedThreadPool( threadFactory );
        Supplier<OutputStream> outputSupplier = new RotatingFileOutputStreamSupplier(
                fs, new File( logFile ), rotationSize, 0, rotationKeepNumber, rotationExecutor );
        FormattedLogProvider logProvider = FormattedLogProvider.withUTCTimeZone().toOutputStream( outputSupplier );
        asyncLogProcessingExecutor = Executors.newSingleThreadExecutor( new NamedThreadFactory( "HTTP-Log-Writer" ) );
        asyncEventProcessor = new AsyncEvents<>( this, this );
        AsyncLogProvider asyncLogProvider = new AsyncLogProvider( asyncEventProcessor, logProvider );
        log = asyncLogProvider.getLog( "REQUEST" );
    }

    @Override
    public void log( Request request, Response response )
    {
        // Trying to replicate this logback pattern:
        // %h %l %user [%t{dd/MMM/yyyy:HH:mm:ss Z}] "%r" %s %b "%i{Referer}" "%i{User-Agent}" %D
        String remoteHost = request.getRemoteHost();
        String user = request.getRemoteUser();
        String requestURL = request.getRequestURI() + "?" + request.getQueryString();
        int statusCode = response.getStatus();
        long length = response.getContentLength();
        String referer = request.getHeader( "Referer" );
        String userAgent = request.getHeader( "User-Agent" );
        long requestTimeStamp = request.getTimeStamp();
        long now = System.currentTimeMillis();
        long serviceTime = requestTimeStamp < 0 ? -1 : now - requestTimeStamp;

        log.info( "%s - %s [%tc] \"%s\" %s %s \"%s\" \"%s\" %s",
                remoteHost, user, now, requestURL, statusCode, length, referer, userAgent, serviceTime );
    }

    @Override
    protected synchronized void doStart() throws Exception
    {
        asyncLogProcessingExecutor.submit( asyncEventProcessor );
        asyncEventProcessor.awaitStartup();
    }

    @Override
    protected synchronized void doStop() throws Exception
    {
        asyncEventProcessor.shutdown();
    }

    @Override
    public void accept( AsyncLogEvent event )
    {
        event.process();
    }

    @Override
    public void eventCount( long count )
    {
    }
}
