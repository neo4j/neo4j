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
package org.neo4j.udc;

import org.neo4j.function.Supplier;

import static org.neo4j.function.Suppliers.singleton;

/**
 * A lookup key to publish or retrieve data in {@link UsageData}.
 * @param <Type>
 */
public class UsageDataKey<Type>
{
    private final String name;

    /** When key is requested and no value exists, a default value is generated and inserted using this */
    private final Supplier<Type> defaultVal;

    public static <T> UsageDataKey<T> key(String name)
    {
        return key(name, null);
    }

    public static <T> UsageDataKey<T> key(String name, T defaultVal)
    {
        return new UsageDataKey<>( name, singleton(defaultVal) );
    }

    public static <T> UsageDataKey<T> key(String name, Supplier<T> defaultVal)
    {
        return new UsageDataKey<>( name, defaultVal );
    }

    public UsageDataKey( String name, Supplier<Type> defaultValue )
    {
        this.name = name;
        this.defaultVal = defaultValue;
    }

    String name()
    {
        return name;
    }

    Type generateDefaultValue()
    {
        return defaultVal == null ? null : defaultVal.get();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        UsageDataKey<?> key = (UsageDataKey<?>) o;

        return name.equals( key.name );

    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
