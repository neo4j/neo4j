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
package org.neo4j.kernel.impl.store.kvstore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class Headers
{
    public abstract <Value> Value get( HeaderField<Value> field );

    public static Builder headersBuilder()
    {
        return new Builder( new HashMap<HeaderField<?>, Object>() );
    }

    public static class Builder
    {
        private final Map<HeaderField<?>, Object> headers;

        Builder( Map<HeaderField<?>, Object> headers )
        {
            this.headers = headers;
        }

        public final <Value> Builder put( HeaderField<Value> field, Value value )
        {
            headers.put( field, value );
            return this;
        }

        @SuppressWarnings("unchecked")
        public final <Value> Value get( HeaderField<Value> field )
        {
            return (Value) headers.get( field );
        }

        public Headers headers()
        {
            return new Simple( new HashMap<>( headers ) );
        }
    }

    <Value> void write( HeaderField<Value> field, BigEndianByteArrayBuffer target )
    {
        field.write( get( field ), target );
    }

    abstract Set<HeaderField<?>> fields();

    private Headers()
    {
        // internal subclasses
    }

    static Headers indexedHeaders( Map<HeaderField<?>, Integer> indexes, Object[] values )
    {
        return new Indexed( indexes, values );
    }

    @Override
    public final int hashCode()
    {
        int hash = 0;
        for ( HeaderField<?> field : fields() )
        {
            hash ^= field.hashCode();
        }
        return hash;
    }

    @Override
    public final boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj instanceof Headers )
        {
            Headers that = (Headers) obj;
            Iterable<HeaderField<?>> these = this.fields(), those = that.fields();
            if ( these.equals( those ) )
            {
                for ( HeaderField<?> field : these )
                {
                    Object tis = this.get( field );
                    Object tat = that.get( field );
                    if ( !tis.equals( tat ) )
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString()
    {
        StringBuilder result = new StringBuilder().append( "Headers{" );
        String pre = "";
        for ( HeaderField<?> field : fields() )
        {
            result.append( pre ).append( field ).append( ": " ).append( get( field ) );
            pre = ", ";
        }
        return result.append( "}" ).toString();
    }

    private static class Indexed extends Headers
    {
        private final Map<HeaderField<?>, Integer> indexes;
        private final Object[] values;

        Indexed( Map<HeaderField<?>, Integer> indexes, Object[] values )
        {
            this.indexes = indexes;
            this.values = values;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Value> Value get( HeaderField<Value> field )
        {
            Integer index = indexes.get( field );
            return index == null ? null : (Value) values[index];
        }

        @Override
        Set<HeaderField<?>> fields()
        {
            return indexes.keySet();
        }
    }

    private static class Simple extends Headers
    {
        private final Map<HeaderField<?>, Object> headers;

        Simple( Map<HeaderField<?>, Object> headers )
        {
            this.headers = headers;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Value> Value get( HeaderField<Value> field )
        {
            return (Value) headers.get( field );
        }

        @Override
        Set<HeaderField<?>> fields()
        {
            return headers.keySet();
        }
    }

    static Map<HeaderField<?>, Object> copy( Headers headers )
    {
        Map<HeaderField<?>, Object> copy = new HashMap<>();
        for ( HeaderField<?> field : headers.fields() )
        {
            copy.put( field, headers.get( field ) );
        }
        return copy;
    }
}
