/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.neo4j.kernel.impl.cache.SizeOfs.sizeOfObject;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;
import static org.neo4j.kernel.impl.cache.SizeOfs.withReference;

abstract class LazyProperty<T> extends DefinedProperty
{
    private volatile Object value;

    LazyProperty( int propertyKeyId, Callable<? extends T> producer )
    {
        super( propertyKeyId );
        this.value = producer;
    }

    @Override
    final boolean hasEqualValue( DefinedProperty that )
    {
        return valueEquals( that.value() );
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
                    this.value = value = produceValue();
                }
            }
        }
        return castAndPrepareForReturn( value );
    }

    protected Object produceValue()
    {
        try
        {
            return ((Callable<?>) value).call();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Casts the internal value to the correct type and makes it ready for returning out,
     * potentially all the way out to the user.
     *
     * @param value the value to cast and prepare.
     * @return the cast and prepared value.
     */
    @SuppressWarnings("unchecked")
    protected T castAndPrepareForReturn( Object value )
    {
        return (T) value;
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        int internalSize = withReference(
                value instanceof Callable<?> ?
                        withObjectOverhead( 0 ) :
                        sizeOfObject( value ) );
        return withObjectOverhead( internalSize );
    }
}
