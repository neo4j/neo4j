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
package org.neo4j.values.storable;

public enum Comparison
{
    LHS_SMALLER_THAN_RHS( 1 ),
    LHS_EQUAL_TO_RHS( 0 ),
    LHS_GREATER_THAN_RHS( -1 );
    private final int cmp;

    Comparison( int cmp )
    {
        this.cmp = cmp;
    }

    public static Comparison comparison( long cmp )
    {
        if ( cmp == 0 )
        {
            return LHS_EQUAL_TO_RHS;
        }
        if ( cmp < 0 )
        {
            return LHS_SMALLER_THAN_RHS;
        }
        else
        {
            return LHS_GREATER_THAN_RHS;
        }
    }
}
