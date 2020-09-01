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
package org.neo4j.bolt.v3.messaging.decoder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AuthToken;

public class HelloMessageDecoder implements RequestMessageDecoder
{
    private final BoltResponseHandler responseHandler;

    public HelloMessageDecoder( BoltResponseHandler responseHandler )
    {
        this.responseHandler = responseHandler;
    }

    @Override
    public int signature()
    {
        return HelloMessage.SIGNATURE;
    }

    @Override
    public BoltResponseHandler responseHandler()
    {
        return responseHandler;
    }

    @Override
    public RequestMessage decode( Neo4jPack.Unpacker unpacker ) throws IOException
    {
        Map<String,Object> meta = readMetaDataMap( unpacker );
        assertUserAgentPresent( meta );
        return new HelloMessage( meta );
    }

    protected static Map<String,Object> readMetaDataMap( Neo4jPack.Unpacker unpacker ) throws IOException
    {
        var metaDataMapValue = unpacker.unpackMap();
        var writer = new PrimitiveOnlyValueWriter();
        var metaDataMap = new HashMap<String,Object>( metaDataMapValue.size() );
        metaDataMapValue.foreach( ( key, value ) ->
        {
            Object convertedValue = AuthToken.containsSensitiveInformation( key ) ?
                                    writer.sensitiveValueAsObject( value ) :
                                    writer.valueAsObject( value );
            metaDataMap.put( key, convertedValue );
        } );
        return metaDataMap;
    }

    private static void assertUserAgentPresent( Map<String,Object> metaData ) throws BoltIOException
    {
        if ( !metaData.containsKey( "user_agent" ) )
        {
            throw new BoltIOException( Status.Request.Invalid, "Expected \"user_agent\" in metadata" );
        }
    }
}
