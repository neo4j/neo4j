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
package org.neo4j.internal.schema;

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Maps;

import java.util.Map;
import java.util.Objects;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;

import static java.util.Collections.unmodifiableMap;

/**
 * The index configuration is an immutable map from Strings to Values.
 * <p>
 * Not all value types are supported, however. Only "storable" values are supported, with the additional restriction that temporal and spatial values are
 * <em>not</em> supported.
 */
public final class IndexConfig
{
    private static final IndexConfig EMPTY = new IndexConfig( Maps.immutable.empty() );

    private final ImmutableMap<String,Value> map;

    private IndexConfig( ImmutableMap<String,Value> map )
    {
        this.map = map;
    }

    public static IndexConfig empty()
    {
        return EMPTY;
    }

    public static IndexConfig with( Map<String,Value> map )
    {
        for ( Value value : map.values() )
        {
            validate( value );
        }
        return new IndexConfig( Maps.immutable.withAll( map ) );
    }

    private static void validate( Value value )
    {
        ValueCategory category = value.valueGroup().category();
        switch ( category )
        {
        case GEOMETRY:
        case GEOMETRY_ARRAY:
        case TEMPORAL:
        case TEMPORAL_ARRAY:
        case UNKNOWN:
        case NO_CATEGORY:
            throw new IllegalArgumentException( "Value type not support in index configuration: " + value + "." );
        default:
            // Otherwise everything is fine.
        }
    }

    public IndexConfig withIfAbsent( String key, Value value )
    {
        validate( value );
        if ( map.containsKey( key ) )
        {
            return this;
        }
        return new IndexConfig( map.newWithKeyValue( key, value ) );
    }

    @SuppressWarnings( "unchecked" )
    public <T extends Value> T get( String key )
    {
        return (T) map.get( key );
    }

    public <T extends Value> T getOrDefault( String key, T defaultValue )
    {
        T value = get( key );
        return value != null ? value : defaultValue;
    }

    public RichIterable<Pair<String, Value>> entries()
    {
        return map.keyValuesView();
    }

    public Map<String,Value> asMap()
    {
        return unmodifiableMap( map.toMap() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof IndexConfig) )
        {
            return false;
        }
        IndexConfig that = (IndexConfig) o;
        return map.equals( that.map );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( map );
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "IndexConfig[" );
        for ( Pair<String,Value> entry : entries() )
        {
            sb.append( entry.getOne() ).append( " -> " ).append( entry.getTwo() ).append( ", " );
        }
        if ( !map.isEmpty() )
        {
            sb.setLength( sb.length() - 2 );
        }
        sb.append( ']' );
        return sb.toString();
    }
}
