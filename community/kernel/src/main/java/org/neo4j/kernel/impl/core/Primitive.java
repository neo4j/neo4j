/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;

abstract class Primitive
{
    protected final long id;

    private ArrayMap<Integer,PropertyData> propertyMap = null;

    protected abstract void changeProperty( NodeManager nodeManager, long propertyId, Object value );

    protected abstract long addProperty( NodeManager nodeManager, PropertyIndex index, Object value );

    protected abstract void removeProperty( NodeManager nodeManager, long propertyId );

    protected abstract ArrayMap<Integer, PropertyData> loadProperties( NodeManager nodeManager,
            boolean light );

    Primitive( long id )
    {
        this.id = id;
    }

    Primitive( long id, boolean newPrimitive )
    {
        this.id = id;
        if ( newPrimitive )
        {
            propertyMap = new ArrayMap<Integer,PropertyData>( 9, false, true );
        }
    }
    
    public long getId()
    {
        return this.id;
    }

    public Iterable<Object> getPropertyValues( NodeManager nodeManager )
    {
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties( nodeManager );
        List<Object> values = new ArrayList<Object>();

        for ( Integer index : propertyMap.keySet() )
        {
            if ( skipMap != null && skipMap.get( index ) != null )
            {
                continue;
            }
            if ( addMap != null && addMap.get( index ) != null )
            {
                continue;
            }
            values.add( propertyMap.get( index ).getValue() );
        }
        if ( addMap != null )
        {
            for ( PropertyData property : addMap.values() )
            {
                values.add( property.getValue() );
            }
        }
        return values;
    }

    public Iterable<String> getPropertyKeys( NodeManager nodeManager )
    {
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties( nodeManager );
        List<String> keys = new ArrayList<String>();

        for ( Integer index : propertyMap.keySet() )
        {
            if ( skipMap != null && skipMap.get( index ) != null )
            {
                continue;
            }
            if ( addMap != null && addMap.get( index ) != null )
            {
                continue;
            }
            keys.add( nodeManager.getIndexFor( index ).getKey() );
        }
        if ( addMap != null )
        {
            for ( Integer index : addMap.keySet() )
            {
                keys.add( nodeManager.getIndexFor( index ).getKey() );
            }
        }
        return keys;
    }

    public Object getProperty( NodeManager nodeManager, String key ) throws NotFoundException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key" );
        }
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties( nodeManager );
        for ( PropertyIndex index : nodeManager.index( key ) )
        {
            if ( skipMap != null && skipMap.get( index.getKeyId() ) != null )
            {
                throw newPropertyNotFoundException( key );
            }
            if ( addMap != null )
            {
                PropertyData property = addMap.get( index.getKeyId() );
                if ( property != null )
                {
                    return getPropertyValue( nodeManager, property );
                }
            }
            PropertyData property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
        }
        PropertyData property = getSlowProperty( nodeManager, addMap, skipMap, key );
        if ( property != null )
        {
            return getPropertyValue( nodeManager, property );
        }
        throw newPropertyNotFoundException( key );
    }
    
    private NotFoundException newPropertyNotFoundException( String key )
    {
        return new NotFoundException( key +
            " property not found for " + this + "." );
    }

    private PropertyData getSlowProperty( NodeManager nodeManager,
            ArrayMap<Integer, PropertyData> addMap,
        ArrayMap<Integer,PropertyData> skipMap, String key )
    {
        if ( nodeManager.hasAllPropertyIndexes() )
        {
            return null;
        }
        if ( addMap != null )
        {
            for ( int keyId : addMap.keySet() )
            {
                if ( !nodeManager.hasIndexFor( keyId ) )
                {
                    PropertyIndex indexToCheck = 
                        nodeManager.getIndexFor( keyId );
                    if ( indexToCheck.getKey().equals( key ) )
                    {
                        if ( skipMap != null && skipMap.get( keyId ) != null )
                        {
                            throw newPropertyNotFoundException( key );
                        }
                        PropertyData property = 
                            addMap.get( indexToCheck.getKeyId() );
                        if ( property != null )
                        {
                            return property;
                        }
                    }
                }
            }
        }
        for ( int keyId : propertyMap.keySet() )
        {
            if ( !nodeManager.hasIndexFor( keyId ) )
            {
                PropertyIndex indexToCheck = nodeManager.getIndexFor( keyId );
                if ( indexToCheck.getKey().equals( key ) )
                {
                    if ( skipMap != null && skipMap.get( keyId ) != null )
                    {
                        throw newPropertyNotFoundException( key );
                    }
                    PropertyData property = propertyMap.get( 
                        indexToCheck.getKeyId() );
                    if ( property != null )
                    {
                        return property;
                    }
                }
            }
        }
        return null;
    }

    public Object getProperty( NodeManager nodeManager, String key, Object defaultValue )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key" );
        }
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties( nodeManager );
        for ( PropertyIndex index : nodeManager.index( key ) )
        {
            if ( skipMap != null && skipMap.get( index.getKeyId() ) != null )
            {
                return defaultValue;
            }
            if ( addMap != null )
            {
                PropertyData property = addMap.get( index.getKeyId() );
                if ( property != null )
                {
                    return getPropertyValue( nodeManager, property );
                }
            }
            PropertyData property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
        }
        PropertyData property = getSlowProperty( nodeManager, addMap, skipMap, key );
        if ( property != null )
        {
            return getPropertyValue( nodeManager, property );
        }
        return defaultValue;
    }

    public boolean hasProperty( NodeManager nodeManager, String key )
    {
        if ( key == null )
        {
            return false;
        }

        ArrayMap<Integer,PropertyData> skipMap =
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties( nodeManager );
        for ( PropertyIndex index : nodeManager.index( key ) )
        {
            if ( skipMap != null && skipMap.get( index.getKeyId() ) != null )
            {
                return false;
            }
            if ( addMap != null )
            {
                PropertyData property = addMap.get( index.getKeyId() );
                if ( property != null )
                {
                    return true;
                }
            }
            PropertyData property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return true;
            }
        }
        PropertyData property = getSlowProperty( nodeManager, addMap, skipMap, key );
        if ( property != null )
        {
            return true;
        }
        return false;
    }

    public void setProperty( NodeManager nodeManager, String key, Object value )
    {
        if ( key == null || value == null )
        {
            throw new IllegalArgumentException( "Null parameter, " + "key=" + 
                key + ", " + "value=" + value );
        }
        nodeManager.acquireLock( this, LockType.WRITE );
        boolean success = false;
        try
        {
            ensureFullProperties( nodeManager );
            ArrayMap<Integer,PropertyData> addMap =
                nodeManager.getCowPropertyAddMap( this, true );
            ArrayMap<Integer,PropertyData> skipMap = 
                nodeManager.getCowPropertyRemoveMap( this );
            PropertyIndex index = null;
            PropertyData property = null;
            boolean foundInSkipMap = false;
            for ( PropertyIndex cachedIndex : nodeManager.index( key ) )
            {
                if ( skipMap != null )
                {
                    if ( skipMap.remove( cachedIndex.getKeyId() ) != null )
                    {
                        foundInSkipMap = true;
                    }
                }
                index = cachedIndex;
                property = addMap.get( cachedIndex.getKeyId() );
                if ( property != null )
                {
                    break;
                }
                property = propertyMap.get( cachedIndex.getKeyId() );
                if ( property != null )
                {
                    break;
                }
            }
            if ( property == null && !nodeManager.hasAllPropertyIndexes() )
            {
                for ( int keyId : addMap.keySet() )
                {
                    if ( !nodeManager.hasIndexFor( keyId ) )
                    {
                        PropertyIndex indexToCheck = nodeManager
                            .getIndexFor( keyId );
                        if ( indexToCheck.getKey().equals( key ) )
                        {
                            if ( skipMap != null )
                            {
                                skipMap.remove( indexToCheck.getKeyId() );
                            }
                            index = indexToCheck;
                            property = addMap.get( indexToCheck.getKeyId() );
                            if ( property != null )
                            {
                                break;
                            }
                        }
                    }
                }
                if ( property == null )
                {
                    for ( int keyId : propertyMap.keySet() )
                    {
                        if ( !nodeManager.hasIndexFor( keyId ) )
                        {
                            PropertyIndex indexToCheck = nodeManager
                                .getIndexFor( keyId );
                            if ( indexToCheck.getKey().equals( key ) )
                            {
                                if ( skipMap != null )
                                {
                                    skipMap.remove( indexToCheck.getKeyId() );
                                }
                                index = indexToCheck;
                                property = propertyMap.get( indexToCheck
                                    .getKeyId() );
                                if ( property != null )
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if ( index == null )
            {
                index = nodeManager.createPropertyIndex( key );
            }
            if ( property != null && !foundInSkipMap )
            {
                long propertyId = property.getId();
                changeProperty( nodeManager, propertyId, value );
                property = new PropertyData( propertyId, value );
            }
            else
            {
                long propertyId = addProperty( nodeManager, index, value );
                property = new PropertyData( propertyId, value );
            }
            addMap.put( index.getKeyId(), property );
            success = true;
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.WRITE );
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    public Object removeProperty( NodeManager nodeManager, String key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Null parameter." );
        }
        nodeManager.acquireLock( this, LockType.WRITE );
        boolean success = false;
        try
        {
            ensureFullProperties( nodeManager );
            PropertyData property = null;
            ArrayMap<Integer,PropertyData> addMap = 
                nodeManager.getCowPropertyAddMap( this );
            ArrayMap<Integer,PropertyData> removeMap =
                nodeManager.getCowPropertyRemoveMap( this, true );
            for ( PropertyIndex cachedIndex : nodeManager.index( key ) )
            {
                if ( addMap != null )
                {
                    property = addMap.remove( cachedIndex.getKeyId() );
                    if ( property != null )
                    {
                        removeMap.put( cachedIndex.getKeyId(), property );
                        break;
                    }
                }
                if ( removeMap.get( cachedIndex.getKeyId() ) != null )
                {
                    success = true;
                    return null;
                }
                property = propertyMap.get( cachedIndex.getKeyId() );
                if ( property != null )
                {
                    removeMap.put( cachedIndex.getKeyId(), property );
                    break;
                }
            }
            if ( property == null && !nodeManager.hasAllPropertyIndexes() )
            {
                if ( addMap != null )
                {
                    for ( int keyId : addMap.keySet() )
                    {
                        if ( !nodeManager.hasIndexFor( keyId ) )
                        {
                            PropertyIndex indexToCheck = nodeManager
                                .getIndexFor( keyId );
                            if ( indexToCheck.getKey().equals( key ) )
                            {
                                property = addMap.remove( indexToCheck
                                    .getKeyId() );
                                if ( property != null )
                                {
                                    removeMap.put( indexToCheck.getKeyId(),
                                        property );
                                    break;
                                }
                            }
                        }
                    }
                    if ( property == null )
                    {
                        for ( int keyId : propertyMap.keySet() )
                        {
                            if ( !nodeManager.hasIndexFor( keyId ) )
                            {
                                PropertyIndex indexToCheck = nodeManager
                                    .getIndexFor( keyId );
                                if ( indexToCheck.getKey().equals( key ) )
                                {
                                    property = propertyMap.get( indexToCheck
                                        .getKeyId() );
                                    if ( property != null )
                                    {
                                        removeMap.put( indexToCheck.getKeyId(),
                                            property );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if ( property == null )
            {
                success = true;
                return null;
            }
            removeProperty( nodeManager, property.getId() );
            success = true;
            return getPropertyValue( nodeManager, property );
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.WRITE );
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    private Object getPropertyValue( NodeManager nodeManager, PropertyData property )
    {
        Object value = property.getValue();
        if ( value == null )
        {
            value = nodeManager.loadPropertyValue( property.getId() );
            property.setNewValue( value );
        }
        return value;
    }

    protected void commitPropertyMaps(
        ArrayMap<Integer,PropertyData> cowPropertyAddMap,
        ArrayMap<Integer,PropertyData> cowPropertyRemoveMap )
    {
        if ( propertyMap == null )
        {
            // we will load full in some other tx
            return;
        }
        if ( cowPropertyAddMap != null )
        {
            for ( Integer index : cowPropertyAddMap.keySet() )
            {
                propertyMap.put( index, cowPropertyAddMap.get( index ) );
            }
        }
        if ( cowPropertyRemoveMap != null && propertyMap != null )
        {
            for ( Integer index : cowPropertyRemoveMap.keySet() )
            {
                propertyMap.remove( index );
            }
        }
    }

    private boolean ensureFullProperties( NodeManager nodeManager )
    {
        if ( propertyMap == null )
        {
            this.propertyMap = loadProperties( nodeManager, false );
            return true;
        }
        return false;
    }

    private boolean ensureFullLightProperties( NodeManager nodeManager )
    {
        if ( propertyMap == null )
        {
            this.propertyMap = loadProperties( nodeManager, true );
            return true;
        }
        return false;
    }

    protected List<PropertyEventData> getAllCommittedProperties( NodeManager nodeManager )
    {
        ensureFullLightProperties( nodeManager );
        List<PropertyEventData> props =
            new ArrayList<PropertyEventData>( propertyMap.size() );
        for ( Map.Entry<Integer,PropertyData> entry : propertyMap.entrySet() )
        {
            PropertyIndex index = nodeManager.getIndexFor( entry.getKey() );
            Object value = getPropertyValue( nodeManager, propertyMap.get( index.getKeyId() ) );
            props.add( new PropertyEventData( index.getKey(), value ) );
        }
        return props;
   }

    protected Object getCommittedPropertyValue( NodeManager nodeManager, String key )
   {
        ensureFullLightProperties( nodeManager );
       for ( PropertyIndex index : nodeManager.index( key ) )
       {
           PropertyData property = propertyMap.get( index.getKeyId() );
           if ( property != null )
           {
                return getPropertyValue( nodeManager, property );
           }
       }
       return null;
   }
}