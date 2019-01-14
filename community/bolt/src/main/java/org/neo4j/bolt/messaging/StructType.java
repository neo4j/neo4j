/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.messaging;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;

import static java.util.Collections.unmodifiableMap;

public enum StructType
{
    NODE( Neo4jPackV1.NODE, "Node" ),
    RELATIONSHIP( Neo4jPackV1.RELATIONSHIP, "Relationship" ),
    UNBOUND_RELATIONSHIP( Neo4jPackV1.UNBOUND_RELATIONSHIP, "Relationship" ),
    PATH( Neo4jPackV1.PATH, "Path" ),
    POINT_2D( Neo4jPackV2.POINT_2D, "Point" ),
    POINT_3D( Neo4jPackV2.POINT_3D, "Point" ),
    DATE( Neo4jPackV2.DATE, "LocalDate" ),
    TIME( Neo4jPackV2.TIME, "OffsetTime" ),
    LOCAL_TIME( Neo4jPackV2.LOCAL_TIME, "LocalTime" ),
    LOCAL_DATE_TIME( Neo4jPackV2.LOCAL_DATE_TIME, "LocalDateTime" ),
    DATE_TIME_WITH_ZONE_OFFSET( Neo4jPackV2.DATE_TIME_WITH_ZONE_OFFSET, "OffsetDateTime" ),
    DATE_TIME_WITH_ZONE_NAME( Neo4jPackV2.DATE_TIME_WITH_ZONE_NAME, "ZonedDateTime" ),
    DURATION( Neo4jPackV2.DURATION, "Duration" );

    private final byte signature;
    private final String description;

    StructType( byte signature, String description )
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

    private static Map<Byte,StructType> knownTypesBySignature = knownTypesBySignature();

    public static StructType valueOf( byte signature )
    {
        return knownTypesBySignature.get( signature );
    }

    public static StructType valueOf( char signature )
    {
        return knownTypesBySignature.get( (byte)signature );
    }

    private static Map<Byte,StructType> knownTypesBySignature()
    {
        StructType[] types = StructType.values();
        Map<Byte,StructType> result = new HashMap<>( types.length * 2 );
        for ( StructType type : types )
        {
            result.put( type.signature, type );
        }
        return unmodifiableMap( result );
    }
}
