/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts.keys;

public enum CountsKeyType
{
    EMPTY( 0 ), ENTITY_NODE( 2 ), ENTITY_RELATIONSHIP( 3 ), INDEX_STATISTICS( 4 ), INDEX_SAMPLE( 5 );

    public final byte code;

    private CountsKeyType( int code )
    {
        this.code = (byte) code;
    }

    public static CountsKeyType fromCode( byte code )
    {
        switch ( code )
        {
            case 0: return EMPTY;
            case 2: return ENTITY_NODE;
            case 3: return ENTITY_RELATIONSHIP;
            case 4: return INDEX_STATISTICS;
            case 5: return INDEX_SAMPLE;

            default:
                throw new IllegalArgumentException( "Invalid counts record type code: " + code );
        }
    }
}
