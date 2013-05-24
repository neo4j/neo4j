/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import java.util.concurrent.Callable;

abstract class LazyProperty<T> extends FullSizeProperty
{
    private volatile Object value;

    LazyProperty( long propertyKeyId, Callable<T> producer )
    {
        super( propertyKeyId );
        this.value = producer;
    }

    @Override
    final boolean hasEqualValue( FullSizeProperty that )
    {
        return valueEquals( ((LazyProperty<?>)that).value() );
    }

    @Override
    public abstract boolean valueEquals( Object value );

    @Override
    public final T value()
    {
        Object value = this.value;
        if ( value instanceof Callable<?> )
        {
            synchronized ( this )
            {
                value = this.value;
                if ( value instanceof Callable<?> )
                {
                    try
                    {
                        this.value = value = ((Callable<?>) value).call();
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            }
        }
        return cast( value );
    }

    @SuppressWarnings("unchecked")
    private T cast( Object value )
    {
        return (T) value;
    }
}
