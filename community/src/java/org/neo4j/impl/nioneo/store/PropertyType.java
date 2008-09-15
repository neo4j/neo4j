/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.nioneo.store;

/**
 * Defines valid property types.
 */
public enum PropertyType
{
    ILLEGAL( 0 ), 
    INT( 1 ), 
    STRING( 2 ), 
    BOOL( 3 ), 
    DOUBLE( 4 ), 
    FLOAT( 5 ),
    LONG( 6 ), 
    BYTE( 7 ), 
    CHAR( 8 ), 
    ARRAY( 9 ), 
    SHORT( 10 );

    private int type;

    PropertyType( int type )
    {
        this.type = type;
    }

    /**
     * Returns an int value representing the type.
     * 
     * @return The int value for this property type
     */
    public int intValue()
    {
        return type;
    }
}