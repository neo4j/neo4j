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

import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

/**
 * A {@link Primitive} which uses a {@link PropertyData}[] for caching properties.
 * It's optimized for a small number of properties and takes less memory than, say
 * a Map based.
 * @author Mattias Persson
 */
abstract class ArrayBasedPrimitive extends Primitive
{
    private volatile PropertyData[] properties;

    ArrayBasedPrimitive( boolean newPrimitive )
    {
        super( newPrimitive );
    }
    
    public int size()
    {
        // properties(PropertyData[])
        int size = 8;
        if ( properties != null )
        {
            size += 16;
            for ( PropertyData data : properties )
            {
                size += data.size();
                size += 8; // array slot
            }
        }
        return size;
    }
    
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
        return result;
    }

    @Override
    public void setProperties( ArrayMap<Integer, PropertyData> properties, NodeManager nodeManager )
    {
        int before = size();
        this.properties = toPropertyArray( properties );
        updateSize( before, size(), nodeManager );
    }

    @Override
    protected PropertyData[] allProperties()
    {
        return properties;
    }

    @Override
    protected PropertyData getPropertyForIndex( int keyId )
    {
        for ( PropertyData property : properties )
        {
            if ( property.getIndex() == keyId )
            {
                return property;
            }
        }
        return null;
    }

    @Override
    protected void commitPropertyMaps(
            ArrayMap<Integer,PropertyData> cowPropertyAddMap,
            ArrayMap<Integer,PropertyData> cowPropertyRemoveMap, long firstProp )
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
                properties = compactedNewArray;
            }
            else
            {
                properties = newArray;
            }
        }
    }
}
