/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * General status codes sent in responses.
 */
public enum StatusCode
{
    SUCCESS( 0 ),
    ONGOING( 1 ),
    FAILURE( -1 );

    private final int codeValue;
    private static AtomicReference<Map<Integer, StatusCode>> codeMap = new AtomicReference<>();

    StatusCode( int codeValue )
    {
        this.codeValue = codeValue;
    }

    public int codeValue()
    {
        return codeValue;
    }

    public static Optional<StatusCode> fromCodeValue( int codeValue )
    {
        Map<Integer,StatusCode> map = codeMap.get();
        if ( map == null )
        {
             map = Stream.of( StatusCode.values() )
                    .collect( Collectors.toMap( StatusCode::codeValue, Function.identity() ) );

            codeMap.compareAndSet( null, map );
        }
        return Optional.ofNullable( map.get( codeValue ) );
    }
}
