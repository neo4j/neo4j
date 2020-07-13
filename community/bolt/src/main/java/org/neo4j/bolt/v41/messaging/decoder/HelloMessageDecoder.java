/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v41.messaging.decoder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.bolt.v41.messaging.request.HelloMessage;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.bolt.v41.messaging.request.HelloMessage.ROUTING;

public class HelloMessageDecoder extends org.neo4j.bolt.v3.messaging.decoder.HelloMessageDecoder
{
    public HelloMessageDecoder( BoltResponseHandler responseHandler )
    {
        super( responseHandler );
    }

    @Override
    public RequestMessage decode( Neo4jPack.Unpacker unpacker ) throws IOException
    {
        Map<String,Object> meta = readMetaDataMap( unpacker );

        RoutingContext routingContext = parseRoutingContext( meta );
        return new HelloMessage( meta, routingContext, extractAuthToken( meta ) );
    }

    @SuppressWarnings( "unchecked" )
    private RoutingContext parseRoutingContext( Map<String,Object> meta ) throws BoltIOException
    {
        Map<String,Object> routingObjectMap = (Map<String,Object>) meta.getOrDefault( ROUTING, null );

        if ( routingObjectMap == null )
        {
            return new RoutingContext( false, Collections.emptyMap() );
        }
        else
        {
            Map<String,String> routingStringMap = new HashMap<>();
            for ( Map.Entry<String,Object> entry : routingObjectMap.entrySet() )
            {
                if ( !(entry.getValue() instanceof String) )
                {
                    throw new BoltIOException( Status.Request.Invalid,
                                               "Routing information in the HELLO message must be a map with string keys and string values." );
                }
                else
                {
                    routingStringMap.put( entry.getKey(), (String) entry.getValue() );
                }
            }
            return new RoutingContext( true, routingStringMap );
        }
    }

    private Map<String, Object> extractAuthToken( Map<String, Object> meta )
    {
        // The authToken is currently nothing more than the Hello metadata minus the routing context.
        Map<String, Object> authToken = new HashMap<>();
        for ( Map.Entry<String, Object> entry : meta.entrySet() )
        {
            if ( !entry.getKey().equals( ROUTING ) )
            {
                authToken.put( entry.getKey(), entry.getValue() );
            }
        }

        return authToken;
    }

}
