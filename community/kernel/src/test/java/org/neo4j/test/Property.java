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
package org.neo4j.test;

import org.neo4j.graphdb.PropertyContainer;

public final class Property
{
    public static Property property( String key, Object value )
    {
        return new Property( key, value );
    }

    public static <E extends PropertyContainer> E set( E entity, Property... properties )
    {
        for ( Property property : properties )
        {
            entity.setProperty( property.key, property.value );
        }
        return entity;
    }

    private final String key;
    private final Object value;

    private Property( String key, Object value )
    {
        this.key = key;
        this.value = value;
    }

    public String key()
    {
        return key;
    }

    public Object value()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return String.format( "%s: %s", key, value );
    }
}
