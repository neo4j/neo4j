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

package org.neo4j.bolt;

import io.netty.channel.Channel;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

import static java.lang.String.format;

/**
 * Logs Bolt messages for a client-server pair.
 */
public class BoltMessageLogger
{
    private final BoltMessageLog messageLog;
    private final Channel channel;
    private final ObjectMapper jsonObjectMapper = new ObjectMapper();

    public BoltMessageLogger( BoltMessageLog messageLog, Channel channel )
    {
        this.messageLog = messageLog;
        this.channel = channel;
    }

    public void clientEvent( String eventName )
    {
        messageLog.info( channel, format( "C: <%s>", eventName ) );
    }

    public void clientEvent( String eventName, Object arg1 )
    {
        messageLog.info( channel, format( "C: <%s>", eventName ), json( arg1 ) );
    }

    public void clientError( String eventName, String arg1 )
    {
        messageLog.error( channel, format( "C: <%s>", eventName ), json( arg1 ) );
    }

    public void clientError( String eventName, String arg1, String arg2 )
    {
        messageLog.error( channel, format( "C: <%s>", eventName ), json( arg1 ), json( arg2 ) );
    }

    public void serverEvent( String eventName )
    {
        messageLog.info( channel, format( "S: <%s>", eventName ) );
    }

    public void serverEvent( String eventName, Object arg1 )
    {
        messageLog.info( channel, format( "S: <%s>", eventName ), json( arg1 ) );
    }

    public void serverError( String eventName, String arg1 )
    {
        messageLog.error( channel, format( "S: <%s>", eventName ), json( arg1 ) );
    }

    public void serverError( String eventName, String arg1, String arg2 )
    {
        messageLog.error( channel, format( "S: <%s>", eventName ), json( arg1 ), json( arg2 ) );
    }

    public void init( String userAgent, Map<String, Object> authToken )
    {
        messageLog.info( channel, "C: INIT", json( userAgent ), "{...}" );
    }

    public void run( String statement, Map<String, Object> parameters )
    {
        messageLog.info( channel, "C: RUN", json( statement ), json( parameters ) );
    }

    public void pullAll()
    {
        messageLog.info( channel, "C: PULL_ALL" );
    }

    public void discardAll()
    {
        messageLog.info( channel, "C: DISCARD_ALL" );
    }

    public void ackFailure()
    {
        messageLog.info( channel, "C: ACK_FAILURE" );
    }

    public void reset()
    {
        messageLog.info( channel, "C: RESET" );
    }

    public void success( Object metadata )
    {
        messageLog.info( channel, "S: SUCCESS", json( metadata ) );
    }

    public void failure( String code, String message )
    {
        messageLog.info( channel, "S: FAILURE", json( code ), json( message ) );
    }

    public void ignored()
    {
        messageLog.info( channel, "S: IGNORED" );
    }

    public void record( Object arg1 )
    {
        messageLog.debug( channel, "S: RECORD", json( arg1 ) );
    }

    private String json( Object arg )
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
