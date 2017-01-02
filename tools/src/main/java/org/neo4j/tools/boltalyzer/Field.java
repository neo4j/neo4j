/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tools.boltalyzer;


import java.util.Map;

/** A mechanism to access a named and typed field in a map without dealing with casting */
public class Field<T>
{
    private final String key;

    Field( String key )
    {
        this.key = key;
    }

    public static <T> Field<T> field( String key )
    {
        return new Field<>( key );
    }

    public T get( Map<String, Object> m )
    {
        return (T) m.get( key );
    }

    public void put( Map<String,Object> m, T val )
    {
        m.put( key, val );
    }
}
