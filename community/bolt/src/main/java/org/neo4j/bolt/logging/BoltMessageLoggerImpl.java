/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

package org.neo4j.bolt.logging;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

/**
 * Logs Bolt messages for a client-server pair.
 */
class BoltMessageLoggerImpl implements BoltMessageLogger
{
    private static final ObjectMapper jsonObjectMapper = new ObjectMapper();
    private final BoltMessageLog messageLog;

    private final String remoteAddress;
    private Channel channel;

    public static final String BOLT_X_CORRELATION_ID_HEADER = "Bolt-X-CorrelationId";
    public static final AttributeKey<String> CORRELATION_ATTRIBUTE_KEY = AttributeKey.valueOf(
            BOLT_X_CORRELATION_ID_HEADER );

    BoltMessageLoggerImpl( BoltMessageLog messageLog, Channel channel )
    {
        this.messageLog = messageLog;
        this.channel = channel;
        this.remoteAddress = remoteAddress( channel );
    }

    @Override
    public void clientEvent( String eventName )
    {
        infoLogger().accept( format( "C: <%s>", eventName ) );
    }

    @Override
    public void clientEvent( String eventName, Supplier<String> detailsSupplier )
    {
        infoLoggerWithArgs().accept( format( "C: <%s>", eventName ), detailsSupplier.get() );
    }

    @Override
    public void clientError( String eventName, String errorMessage, Supplier<String> detailsSupplier )
    {
        errorLoggerWithArgs( errorMessage ).accept( format( "C: <%s>", eventName ), detailsSupplier.get() );
    }

    @Override
    public void serverEvent( String eventName )
    {
        infoLogger().accept( format( "S: <%s>", eventName ) );
    }

    @Override
    public void serverEvent( String eventName, Supplier<String> detailsSupplier )
    {
        infoLoggerWithArgs().accept( format( "S: <%s>", eventName ), detailsSupplier.get() );
    }

    @Override
    public void serverError( String eventName, String errorMessage )
    {
        errorLogger( errorMessage ).accept( format( "S: <%s>", eventName ) );
    }

    @Override
    public void serverError( String eventName, Status status, String errorMessage )
    {
        errorLoggerWithArgs( errorMessage ).accept( format( "S: <%s>", eventName ), status.code().serialize() );
    }

    @Override
    public void logInit( String userAgent, Map<String,Object> authToken )
    {
        // log only auth toke keys, not values that include password
        infoLoggerWithArgs().accept( "C: INIT", format( "%s %s", userAgent, json( authToken.keySet() ) ) );
    }

    @Override
    public void logRun( String statement, Map<String,Object> parameters )
    {
        infoLoggerWithArgs().accept( "C: RUN", format( "%s %s", statement, json( parameters ) ) );
    }

    @Override
    public void logPullAll()
    {
        infoLogger().accept( "C: PULL_ALL" );
    }

    @Override
    public void logDiscardAll()
    {
        infoLogger().accept( "C: DISCARD_ALL" );
    }

    @Override
    public void logAckFailure()
    {
        infoLogger().accept( "C: ACK_FAILURE" );
    }

    @Override
    public void logReset()
    {
        infoLogger().accept( "C: RESET" );
    }

    @Override
    public void logSuccess( Object metadata )
    {
        infoLoggerWithArgs().accept( "S: SUCCESS", json( metadata ) );
    }

    @Override
    public void logFailure( Status status, String errorMessage )
    {
        infoLoggerWithArgs().accept( "S: FAILURE", format( "%s %s", status.code().serialize(), errorMessage ) );
    }

    @Override
    public void logIgnored()
    {
        infoLogger().accept( "S: IGNORED" );
    }

    private Consumer<String> infoLogger()
    {
        String correlationId;
        if ( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) )
        {
            String boltXCorrelationIdValue = channel.attr( CORRELATION_ATTRIBUTE_KEY ).get();
            correlationId = format( BOLT_X_CORRELATION_ID_HEADER + ": %s", boltXCorrelationIdValue );
            if ( StringUtils.isNotBlank( correlationId ) )
            {
                return formatMessageWithEventName ->
                        messageLog.info( remoteAddress, formatMessageWithEventName, correlationId );
            }
        }
        return formatMessageWithEventName -> messageLog.info( remoteAddress, formatMessageWithEventName );
    }

    private BiConsumer<String, String> infoLoggerWithArgs()
    {
        return ( formatMessageWithEventName, details ) ->
                infoLogger().accept( format( "%s %s", formatMessageWithEventName, details ) );
    }

    private Consumer<String> errorLogger( String errorMessage )
    {

        String correlationId;
        if ( channel.hasAttr( CORRELATION_ATTRIBUTE_KEY ) )
        {
            String boltXCorrelationIdValue = channel.attr( CORRELATION_ATTRIBUTE_KEY ).get();
            correlationId = format( BOLT_X_CORRELATION_ID_HEADER + ": %s", boltXCorrelationIdValue );
            if ( StringUtils.isNotBlank( correlationId ) )
            {
                return formatMessageWithEventName ->
                        messageLog.error( remoteAddress, errorMessage, formatMessageWithEventName, correlationId );
            }
        }
        return formatMessageWithEventName ->
                messageLog.error( remoteAddress, errorMessage, formatMessageWithEventName );
    }

    private BiConsumer<String, String> errorLoggerWithArgs( String errorMessage )
    {
        return ( formatMessageWithEventName, details ) ->
                errorLogger( errorMessage ).accept( format( "%s %s", formatMessageWithEventName, details ) );
    }

    private static String remoteAddress( Channel channel )
    {
        return channel.remoteAddress().toString();
    }

    private static String json( Object arg )
    {
        try
        {
            return jsonObjectMapper.writeValueAsString( arg );
        }
        catch ( IOException e )
        {
            return "?";
        }
    }

}
