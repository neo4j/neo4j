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

import io.netty.channel.Channel;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

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

    BoltMessageLoggerImpl( BoltMessageLog messageLog, Channel channel )
    {
        this.messageLog = messageLog;
        this.remoteAddress = remoteAddress( channel );
    }

    @Override
    public void clientEvent( String eventName )
    {
        messageLog.info( remoteAddress, format( "C: <%s>", eventName ) );
    }

    @Override
    public void clientEvent( String eventName, Supplier<String> detailsSupplier )
    {
        messageLog.info( remoteAddress, format( "C: <%s>", eventName ), detailsSupplier.get() );
    }

    @Override
    public void clientError( String eventName, String message, Supplier<String> detailsSupplier )
    {
        messageLog.error( remoteAddress, format( "C: <%s>", eventName ), message, detailsSupplier.get() );
    }

    @Override
    public void serverEvent( String eventName )
    {
        messageLog.info( remoteAddress, format( "S: <%s>", eventName ) );
    }

    @Override
    public void serverEvent( String eventName, Supplier<String> detailsSupplier )
    {
        messageLog.info( remoteAddress, format( "S: <%s>", eventName ), detailsSupplier.get() );
    }

    @Override
    public void serverError( String eventName, String message )
    {
        messageLog.error( remoteAddress, format( "S: <%s>", eventName ), message );
    }

    @Override
    public void serverError( String eventName, Status status, String message )
    {
        messageLog.error( remoteAddress, format( "S: <%s>", eventName ), status.code().serialize(), message );
    }

    @Override
    public void init( String userAgent, Map<String,Object> authToken )
    {
        // log only auth toke keys, not values that include password
        messageLog.info( remoteAddress, "C: INIT", userAgent, json( authToken.keySet() ) );
    }

    @Override
    public void run( String statement, Map<String,Object> parameters )
    {
        messageLog.info( remoteAddress, "C: RUN", statement, json( parameters ) );
    }

    @Override
    public void pullAll()
    {
        messageLog.info( remoteAddress, "C: PULL_ALL" );
    }

    @Override
    public void discardAll()
    {
        messageLog.info( remoteAddress, "C: DISCARD_ALL" );
    }

    @Override
    public void ackFailure()
    {
        messageLog.info( remoteAddress, "C: ACK_FAILURE" );
    }

    @Override
    public void reset()
    {
        messageLog.info( remoteAddress, "C: RESET" );
    }

    @Override
    public void success( Object metadata )
    {
        messageLog.info( remoteAddress, "S: SUCCESS", json( metadata ) );
    }

    @Override
    public void failure( Status status, String message )
    {
        messageLog.info( remoteAddress, "S: FAILURE", status.code().serialize(), message );
    }

    @Override
    public void ignored()
    {
        messageLog.info( remoteAddress, "S: IGNORED" );
    }

    @Override
    public void record( Object arg1 )
    {
        messageLog.debug( remoteAddress, "S: RECORD", json( arg1 ) );
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
