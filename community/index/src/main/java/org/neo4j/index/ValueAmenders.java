/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index;

public class ValueAmenders
{
    @SuppressWarnings( "rawtypes" )
    private static final ValueAmender OVERWRITE = new ValueAmender()
    {
        @Override
        public Object amend( Object value, Object withValue )
        {
            return withValue;
        }
    };

    @SuppressWarnings( "rawtypes" )
    private static final ValueAmender INSERT_NEW = new ValueAmender()
    {
        @Override
        public Object amend( Object value, Object withValue )
        {
            return null;
        }
    };

    @SuppressWarnings( "unchecked" )
    public static <VALUE> ValueAmender<VALUE> overwrite()
    {
        return OVERWRITE;
    }

    @SuppressWarnings( "unchecked" )
    public static <VALUE> ValueAmender<VALUE> insertNew()
    {
        return INSERT_NEW;
    }
}
