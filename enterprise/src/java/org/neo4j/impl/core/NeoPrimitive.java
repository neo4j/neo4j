/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.api.core.NotFoundException;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayMap;

abstract class NeoPrimitive
{
    protected final int id;
    protected final NodeManager nodeManager;

    private ArrayMap<Integer,PropertyData> propertyMap = null;
    
    protected abstract void changeProperty( int propertyId, Object value );

    protected abstract int addProperty( PropertyIndex index, Object value );

    protected abstract void removeProperty( int propertyId );

    protected abstract ArrayMap<Integer,PropertyData> loadProperties();

    NeoPrimitive( int id, NodeManager nodeManager )
    {
        this.id = id;
        this.nodeManager = nodeManager;
    }

    NeoPrimitive( int id, boolean newPrimitive, NodeManager nodeManager )
    {
        this.id = id;
        this.nodeManager = nodeManager;
        if ( newPrimitive )
        {
            propertyMap = new ArrayMap<Integer,PropertyData>( 9, false, true );
        }
    }
    
    public long getId()
    {
        return this.id;
    }

    public Iterable<Object> getPropertyValues()
    {
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties();
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

    public Iterable<String> getPropertyKeys()
    {
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties();
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

    public Object getProperty( String key ) throws NotFoundException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key" );
        }
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties();
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
                    return getPropertyValue( property );
                }
            }
            PropertyData property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( property );
            }
        }
        PropertyData property = getSlowProperty( addMap, skipMap, key );
        if ( property != null )
        {
            return getPropertyValue( property );
        }
        throw newPropertyNotFoundException( key );
    }
    
    private NotFoundException newPropertyNotFoundException( String key )
    {
        return new NotFoundException( key +
            " property not found for " + this + "." );
    }
    
    private PropertyData getSlowProperty( ArrayMap<Integer,PropertyData> addMap,
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

    public Object getProperty( String key, Object defaultValue )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key" );
        }
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties();
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
                    return getPropertyValue( property );
                }
            }
            PropertyData property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( property );
            }
        }
        PropertyData property = getSlowProperty( addMap, skipMap, key );
        if ( property != null )
        {
            return getPropertyValue( property );
        }
        return defaultValue;
    }

    public boolean hasProperty( String key )
    {
        if ( key == null )
        {
            return false;
        }
        
        ArrayMap<Integer,PropertyData> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,PropertyData> addMap = 
            nodeManager.getCowPropertyAddMap( this );

        ensureFullProperties();
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
        PropertyData property = getSlowProperty( addMap, skipMap, key );
        if ( property != null )
        {
            return true;
        }
        return false;
    }

    public void setProperty( String key, Object value )
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
            ensureFullProperties();
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
                int propertyId = property.getId();
                changeProperty( propertyId, value );
                property.setNewValue( value );
            }
            else
            {
                int propertyId = addProperty( index, value );
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
                setRollbackOnly();
            }
        }
    }

    public Object removeProperty( String key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Null parameter." );
        }
        nodeManager.acquireLock( this, LockType.WRITE );
        boolean success = false;
        try
        {
            ensureFullProperties();
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
            removeProperty( property.getId() );
            success = true;
            return getPropertyValue( property );
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.WRITE );
            if ( !success )
            {
                setRollbackOnly();
            }
        }
    }

    private Object getPropertyValue( PropertyData property )
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
    
    private boolean ensureFullProperties()
    {
        if ( propertyMap == null )
        {
            this.propertyMap = loadProperties();
            return true;
        }
        return false;
    }

    protected void setRollbackOnly()
    {
        nodeManager.setRollbackOnly();
    }
}