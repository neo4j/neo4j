/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.messaging;

import java.util.HashMap;
import java.util.Map;

public enum KnownType
{
    NODE( 'N', "Node" ),
    RELATIONSHIP( 'R', "Relationship" ),
    UNBOUND_RELATIONSHIP( 'r', "Relationship" ),
    PATH( 'P', "Path" ),
    POINT_2D( 'X', "Point" ),
    POINT_3D( 'Y', "Point" ),
    DATE( 'D', "LocalDate" ),
    TIME( 'T', "OffsetTime" ),
    LOCAL_TIME( 't', "LocalTime" ),
    LOCAL_DATE_TIME( 'd', "LocalDateTime" ),
    DATE_TIME_WITH_ZONE_OFFSET( 'F', "OffsetDateTime" ),
    DATE_TIME_WITH_ZONE_NAME( 'f', "ZonedDateTime" ),
    DURATION( 'E', "Duration" );

    private final byte signature;
    private final String description;

    KnownType( char signature, String description )
    {
        this( (byte)signature, description );
    }

    KnownType( byte signature, String description )
    {
        this.signature = signature;
        this.description = description;
    }

    public byte signature()
    {
        return signature;
    }

    public String description()
    {
        return description;
    }

    private static Map<Byte, KnownType> byteToKnownTypeMap = new HashMap<>();
    static
    {
        for ( KnownType type : KnownType.values() )
        {
            byteToKnownTypeMap.put( type.signature, type );
        }
    }

    public static KnownType valueOf( byte signature )
    {
        return byteToKnownTypeMap.get( signature );
    }

    public static KnownType valueOf( char signature )
    {
        return KnownType.valueOf( (byte)signature );
    }
}
