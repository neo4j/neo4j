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
package org.neo4j.kernel.impl.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.cache.EntityWithSizeObject;
import org.neo4j.kernel.impl.cache.SizeOfs;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverheadIncludingReferences;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;

/**
 * A {@link Primitive} which uses a {@link PropertyData}[] for caching properties.
 * It's optimized for a small number of properties and takes less memory than, say
 * a Map based.
 * @author Mattias Persson
 */
abstract class ArrayBasedPrimitive extends Primitive implements EntityWithSizeObject
{
    private volatile Property[] properties;
    private volatile int registeredSize;

    ArrayBasedPrimitive( boolean newPrimitive )
    {
        super( newPrimitive );
    }
    
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
        int size = SizeOfs.REFERENCE_SIZE/*properties reference*/ + 8/*registered size*/;
        if ( properties != null )
        {
            size = withArrayOverheadIncludingReferences( size, properties.length ); // the actual properties[] object
            for ( Property data : properties )
                size += data.asPropertyDataJustForIntegration().sizeOfObjectInBytesIncludingOverhead();
        }
        return withObjectOverhead( size );
    }
    
    @Override
    protected void setEmptyProperties()
    {
        properties = NO_PROPERTIES;
    }

    private Property[] toPropertyArray( Collection<Property> loadedProperties )
    {
        if ( loadedProperties == null || loadedProperties.size() == 0 )
        {
            return NO_PROPERTIES;
        }

        Property[] result = new Property[loadedProperties.size()];
        int i = 0;
        for ( Property property : loadedProperties )
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
    
    private static final Comparator<Property> PROPERTY_DATA_COMPARATOR_FOR_SORTING = new Comparator<Property>()
    {
        @Override
        public int compare( Property o1, Property o2 )
        {
            return (int) o1.propertyKeyId() - (int) o2.propertyKeyId();
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
        public int compare( Object o1, Object o2 )
        {
            return (int) ((Property)o1).propertyKeyId() - ((Integer) o2).intValue();
        }
    };
    
    @Override
    protected boolean hasLoadedProperties()
    {
        return properties != null;
    }
    
    @Override
    protected Iterator<Property> getCachedProperties()
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
    
    protected abstract Property noProperty( long key );

    @Override
    protected void setProperties( Iterator<Property> properties )
    {
        this.properties = toPropertyArray( asCollection( properties ) );
    }

    @Override
    protected PropertyData getPropertyForIndex( int keyId )
    {
        Property[] localProperties = properties;
        int index = Arrays.binarySearch( localProperties, keyId, PROPERTY_DATA_COMPARATOR_FOR_BINARY_SEARCH );
        return index < 0 ? null : localProperties[index].asPropertyDataJustForIntegration();
    }

    @Override
    protected void commitPropertyMaps(
            ArrayMap<Integer, PropertyData> cowPropertyAddMap,
            ArrayMap<Integer, PropertyData> cowPropertyRemoveMap, long firstProp )
    {
        synchronized ( this )
        {
            // Dereference the volatile once to avoid multiple barriers
            Property[] newArray = properties;
            if ( newArray == null ) return;

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
                Property[] oldArray = newArray;
                newArray = new Property[oldArray.length + extraLength];
                System.arraycopy( oldArray, 0, newArray, 0, oldArray.length );
            }
            else
            {
                newArray = newArray.clone();
            }

            if ( cowPropertyRemoveMap != null )
            {
                for ( Integer keyIndex : cowPropertyRemoveMap.keySet() )
                {
                    for ( int i = 0; i < newArraySize; i++ )
                    {
                        Property existingProperty = newArray[i];
                        if ( existingProperty.propertyKeyId() == keyIndex )
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
                for ( PropertyData addedProperty : cowPropertyAddMap.values() )
                {
                    for ( int i = 0; i < newArray.length; i++ )
                    {
                        Property existingProperty = newArray[i];
                        if ( existingProperty == null || addedProperty.getIndex() == existingProperty.propertyKeyId() )
                        {
                            newArray[i] = Property.property( addedProperty.getIndex(), addedProperty.getValue() );
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
                Property[] compactedNewArray = new Property[newArraySize];
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
