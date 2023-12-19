/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
