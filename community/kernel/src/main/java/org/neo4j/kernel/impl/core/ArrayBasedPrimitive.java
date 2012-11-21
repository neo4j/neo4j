/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.kernel.impl.cache.SizeOfs.withArrayOverheadIncludingReferences;
import static org.neo4j.kernel.impl.cache.SizeOfs.withObjectOverhead;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.kernel.impl.cache.EntityWithSize;
import org.neo4j.kernel.impl.cache.SizeOfs;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

/**
 * A {@link Primitive} which uses a {@link PropertyData}[] for caching properties.
 * It's optimized for a small number of properties and takes less memory than, say
 * a Map based.
 * @author Mattias Persson
 */
abstract class ArrayBasedPrimitive extends Primitive implements EntityWithSize
{
    private volatile PropertyData[] properties;
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
    
    public int size()
    {
        int size = SizeOfs.REFERENCE_SIZE/*properties reference*/ + 8/*registered size*/;
        if ( properties != null )
        {
            size = withArrayOverheadIncludingReferences( size, properties.length ); // the actual properties[] object
            for ( PropertyData data : properties )
                size += data.size();
        }
        return withObjectOverhead( size );
    }
    
    abstract protected void updateSize( NodeManager nodeManager );
    
    @Override
    protected void setEmptyProperties()
    {
        properties = NO_PROPERTIES;
    }

    private PropertyData[] toPropertyArray( ArrayMap<Integer, PropertyData> loadedProperties )
    {
        if ( loadedProperties == null || loadedProperties.size() == 0 )
        {
            return NO_PROPERTIES;
        }

        PropertyData[] result = new PropertyData[loadedProperties.size()];
        int i = 0;
        for ( PropertyData property : loadedProperties.values() )
        {
            result[i++] = property;
        }
        sort( result );
        return result;
    }
    
    private static void sort( PropertyData[] array )
    {
        Arrays.sort( array, PROPERTY_DATA_COMPARATOR_FOR_SORTING );
    }
    
    private static final Comparator<PropertyData> PROPERTY_DATA_COMPARATOR_FOR_SORTING = new Comparator<PropertyData>()
    {
        @Override
        public int compare( PropertyData o1, PropertyData o2 )
        {
            return o1.getIndex() - o2.getIndex();
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
            return ((PropertyData)o1).getIndex() - ((Integer) o2).intValue();
        }
    };

    @Override
    public void setProperties( ArrayMap<Integer, PropertyData> properties, NodeManager nodeManager )
    {
        this.properties = toPropertyArray( properties );
        updateSize( nodeManager );
    }

    @Override
    protected PropertyData[] allProperties()
    {
        return properties;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected PropertyData getPropertyForIndex( int keyId )
    {
        PropertyData[] localProperties = properties;
        int index = Arrays.binarySearch( localProperties, keyId, PROPERTY_DATA_COMPARATOR_FOR_BINARY_SEARCH );
        return index < 0 ? null : localProperties[index];
    }

    @Override
    protected void commitPropertyMaps(
            ArrayMap<Integer,PropertyData> cowPropertyAddMap,
            ArrayMap<Integer,PropertyData> cowPropertyRemoveMap, long firstProp, NodeManager nodeManager )
    {
        synchronized ( this )
        {
            // Dereference the volatile once to avoid multiple barriers
            PropertyData[] newArray = properties;
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
                PropertyData[] oldArray = newArray;
                newArray = new PropertyData[oldArray.length + extraLength];
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
                        PropertyData existingProperty = newArray[i];
                        if ( existingProperty.getIndex() == keyIndex )
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
                        PropertyData existingProperty = newArray[i];
                        if ( existingProperty == null || addedProperty.getIndex() == existingProperty.getIndex() )
                        {
                            newArray[i] = addedProperty;
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
                PropertyData[] compactedNewArray = new PropertyData[newArraySize];
                System.arraycopy( newArray, 0, compactedNewArray, 0, newArraySize );
                sort( compactedNewArray );
                properties = compactedNewArray;
            }
            else
            {
                sort( newArray );
                properties = newArray;
            }
            updateSize( nodeManager );
        }
    }
}
