/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

public enum CountsRecordType
{
    EMPTY( 0 ), NODE( 1 ), RELATIONSHIP( 2 ), INDEX( 4 );

    public final byte code;

    private CountsRecordType( int code )
    {
        this.code = (byte) code;
    }

    public static CountsRecordType fromCode( byte code )
    {
        switch ( code )
        {
            case 0: return EMPTY;
            case 1: return NODE;
            case 2: return RELATIONSHIP;
            case 4: return INDEX;
            default:
                throw new IllegalArgumentException( "Invalid counts record type code: " + code );
        }
    }
}
