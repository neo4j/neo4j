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
package org.neo4j.kernel.impl.core;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.kernel.impl.cache.SizeOfs.REFERENCE_SIZE;
import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverheadIncludingReferences;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.cache.EntityWithSizeObject;

/**
 * A {@link Primitive} which uses a {@link DefinedProperty}[] for caching properties.
 * It's optimized for a small number of properties and takes less memory than, say
 * a Map based.
 * @author Mattias Persson
 */
abstract class ArrayBasedPrimitive extends Primitive implements EntityWithSizeObject
{
    private volatile DefinedProperty[] properties;
    private volatile int registeredSize;

    @Override
    public void setRegisteredSize( int size )
    {
        this.registeredSize = size;
    }

    @Override
    public int getRegisteredSize()
    {
        return registeredSize;
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        int size = REFERENCE_SIZE/*properties reference*/ + 8/*registered size*/;
        if ( properties != null && properties.length > 0 )
        {
            size = withArrayOverheadIncludingReferences( size, properties.length ); // the actual properties[] object
            for ( DefinedProperty data : properties )
            {
                size += data.sizeOfObjectInBytesIncludingOverhead();
            }
        }
        return withObjectOverhead( size );
    }

    private DefinedProperty[] toPropertyArray(
            Collection<DefinedProperty> loadedProperties )
    {
        if ( loadedProperties == null || loadedProperties.size() == 0 )
        {
            return NO_PROPERTIES;
        }

        DefinedProperty[] result = new DefinedProperty[loadedProperties.size()];
        int i = 0;
        for ( DefinedProperty property : loadedProperties )
        {
            result[i++] = property;
        }
        sort( result );
        return result;
    }

    private static void sort( Property[] array )
    {
        Arrays.sort( array, PROPERTY_DATA_COMPARATOR_FOR_SORTING );
    }

    static final Comparator<Property> PROPERTY_DATA_COMPARATOR_FOR_SORTING = new Comparator<Property>()
    {
        @Override
        public int compare( Property o1, Property o2 )
        {
            return o1.propertyKeyId() - o2.propertyKeyId();
        }
    };

    /* This is essentially a deliberate misuse of Comparator, knowing details about Arrays#binarySearch.
     * The signature is binarySearch( T[] array, T key, Comparator<T> ), but in this case we're
     * comparing PropertyData[] to an int as key. To avoid having to create a new object for
     * the key for each call we create a single Comparator taking the PropertyData as first
     * argument and the key as the second, as #binarySearch does internally. Although the int
     * here will be boxed I imagine it to be slightly better, with Integer caching for low
     * integers. */
    @SuppressWarnings( "rawtypes" )
    static final Comparator PROPERTY_DATA_COMPARATOR_FOR_BINARY_SEARCH = new Comparator()
    {
        @Override
        @SuppressWarnings("UnnecessaryUnboxing")
        public int compare( Object o1, Object o2 )
        {
            return ((Property)o1).propertyKeyId() - ((Integer) o2).intValue();
        }
    };

    @Override
    protected boolean hasLoadedProperties()
    {
        return properties != null;
    }

    @Override
    protected Iterator<DefinedProperty> getCachedProperties()
    {
        return iterator( properties );
    }

    @Override
    protected PrimitiveLongIterator getCachedPropertyKeys()
    {
        return new PrimitiveLongIterator()
        {
            private final Property[] localProperties = properties;
            private int i;

            @Override
            public long next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return localProperties[i++].propertyKeyId();
            }

            @Override
            public boolean hasNext()
            {
                return i < localProperties.length;
            }
        };
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Property getCachedProperty( int key )
    {
        Property[] localProperties = properties;
        int index = Arrays.binarySearch( localProperties, key, PROPERTY_DATA_COMPARATOR_FOR_BINARY_SEARCH );
        return index < 0 ? noProperty( key ) : localProperties[index];
    }

    protected abstract Property noProperty( int key );

    @Override
    protected void setProperties( Iterator<DefinedProperty> properties )
    {
        this.properties = toPropertyArray( asCollection( properties ) );
    }

    @Override
    public void commitPropertyMaps(
            PrimitiveIntObjectMap<DefinedProperty> cowPropertyAddMap, Iterator<Integer> removed )
    {
        synchronized ( this )
        {
            // Dereference the volatile once to avoid multiple barriers
            DefinedProperty[] newArray = properties;
            if ( newArray == null )
            {
                return;
            }

            /*
             * add map will definitely be added in the properties array - all properties
             * added and later removed in the same tx are removed from there as well.
             * The remove map will not necessarily be removed, since it may hold a prop that was
             * added in this tx. So the difference in size is all the keys that are common
             * between properties and remove map subtracted by the add map size.
             */
            int extraLength = 0;
            if (cowPropertyAddMap != null)
            {
                extraLength += cowPropertyAddMap.size();
            }

            int newArraySize = newArray.length;

            // make sure that we don't make inplace modifications to the existing array
            // TODO: Refactor this to guarantee only one copy,
            // currently it can do two copies in the clone() case if it also compacts
            if ( extraLength > 0 )
            {
                DefinedProperty[] oldArray = newArray;
                newArray = new DefinedProperty[oldArray.length + extraLength];
                System.arraycopy( oldArray, 0, newArray, 0, oldArray.length );
            }
            else
            {
                newArray = newArray.clone();
            }

            if ( removed != null )
            {
                while ( removed.hasNext() )
                {
                    int key = removed.next();
                    for ( int i = 0; i < newArraySize; i++ )
                    {
                        Property existingProperty = newArray[i];
                        if ( existingProperty.propertyKeyId() == key )
                        {
                            int swapWith = --newArraySize;
                            newArray[i] = newArray[swapWith];
                            newArray[swapWith] = null;
                            break;
                        }
                    }
                }
            }

            if ( cowPropertyAddMap != null )
            {
                PrimitiveIntIterator keyIterator = cowPropertyAddMap.iterator();
                while ( keyIterator.hasNext() )
                {
                    int key = keyIterator.next();
                    DefinedProperty addedProperty = cowPropertyAddMap.get( key );
                    for ( int i = 0; i < newArray.length; i++ )
                    {
                        Property existingProperty = newArray[i];
                        if ( existingProperty == null || addedProperty.propertyKeyId() == existingProperty.propertyKeyId() )
                        {
                            newArray[i] = Property.property( addedProperty.propertyKeyId(), addedProperty.value() );
                            if ( existingProperty == null )
                            {
                                newArraySize++;
                            }
                            break;
                        }
                    }
                }
            }

            // these size changes are updated from lock releaser
            if ( newArraySize < newArray.length )
            {
                DefinedProperty[] compactedNewArray = new DefinedProperty[newArraySize];
                System.arraycopy( newArray, 0, compactedNewArray, 0, newArraySize );
                sort( compactedNewArray );
                properties = compactedNewArray;
            }
            else
            {
                sort( newArray );
                properties = newArray;
            }
        }
    }
}
