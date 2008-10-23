/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayIntSet;
import org.neo4j.impl.util.ArrayMap;

abstract class NeoPrimitive
{
    private static enum PropertyPhase
    {
        EMPTY_PROPERTY, FULL_PROPERTY,
    }

    protected final int id;
    protected final NodeManager nodeManager;

    private PropertyPhase propPhase;
    private ArrayMap<Integer,Property> propertyMap = null;
    
    protected abstract void changeProperty( int propertyId, Object value );

    protected abstract int addProperty( PropertyIndex index, Object value );

    protected abstract void removeProperty( int propertyId );

    protected abstract RawPropertyData[] loadProperties();

    NeoPrimitive( int id, NodeManager nodeManager )
    {
        this.id = id;
        this.nodeManager = nodeManager;
        this.propPhase = PropertyPhase.EMPTY_PROPERTY;
    }

    NeoPrimitive( int id, boolean newPrimitive, NodeManager nodeManager )
    {
        this.id = id;
        this.nodeManager = nodeManager;
        if ( newPrimitive )
        {
            this.propPhase = PropertyPhase.FULL_PROPERTY;
        }
    }
    
    public long getId()
    {
        return this.id;
    }

    public Iterable<Object> getPropertyValues()
    {
        ArrayMap<Integer,Property> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,Property> addMap = 
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
            for ( Property property : addMap.values() )
            {
                values.add( property.getValue() );
            }
        }
        return values;
    }

    public Iterable<String> getPropertyKeys()
    {
        ArrayMap<Integer,Property> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,Property> addMap = 
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
        ArrayMap<Integer,Property> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,Property> addMap = 
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
                Property property = addMap.get( index.getKeyId() );
                if ( property != null )
                {
                    return getPropertyValue( property );
                }
            }
            Property property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( property );
            }
        }
        Property property = getSlowProperty( addMap, skipMap, key );
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
    
    private Property getSlowProperty( ArrayMap<Integer,Property> addMap,
        ArrayMap<Integer,Property> skipMap, String key )
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
                        Property property = 
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
                    Property property = propertyMap.get( 
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
        ArrayMap<Integer,Property> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,Property> addMap = 
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
                Property property = addMap.get( index.getKeyId() );
                if ( property != null )
                {
                    return getPropertyValue( property );
                }
            }
            Property property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( property );
            }
        }
        Property property = getSlowProperty( addMap, skipMap, key );
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
        
        ArrayMap<Integer,Property> skipMap = 
            nodeManager.getCowPropertyRemoveMap( this );
        ArrayMap<Integer,Property> addMap = 
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
                Property property = addMap.get( index.getKeyId() );
                if ( property != null )
                {
                    return true;
                }
            }
            Property property = propertyMap.get( index.getKeyId() );
            if ( property != null )
            {
                return true;
            }
        }
        Property property = getSlowProperty( addMap, skipMap, key );
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
            ArrayMap<Integer,Property> addMap = 
                nodeManager.getCowPropertyAddMap( this, true );
            ArrayMap<Integer,Property> skipMap = 
                nodeManager.getCowPropertyRemoveMap( this );
            PropertyIndex index = null;
            Property property = null;
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
                property = new Property( propertyId, value );
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
            Property property = null;
            ArrayMap<Integer,Property> addMap = 
                nodeManager.getCowPropertyAddMap( this );
            ArrayMap<Integer,Property> removeMap =
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

    private Object getPropertyValue( Property property )
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
        ArrayMap<Integer,Property> cowPropertyAddMap, 
        ArrayMap<Integer,Property> cowPropertyRemoveMap )
    {
        if ( cowPropertyAddMap != null )
        {
            if ( propertyMap == null )
            {
                propertyMap = new ArrayMap<Integer,Property>();
            }
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
        if ( propPhase != PropertyPhase.FULL_PROPERTY )
        {
            RawPropertyData[] rawProperties = loadProperties();
            ArrayIntSet addedProps = new ArrayIntSet();
            ArrayMap<Integer,Property> newPropertyMap = 
                new ArrayMap<Integer,Property>();
            for ( RawPropertyData propData : rawProperties )
            {
                int propId = propData.getId();
                assert addedProps.add( propId );
                Property property = new Property( propId, propData.getValue() );
                newPropertyMap.put( propData.getIndex(), property );
            }
            if ( propertyMap != null )
            {
                for ( int index : this.propertyMap.keySet() )
                {
                    Property prop = propertyMap.get( index );
                    if ( !addedProps.contains( prop.getId() ) )
                    {
                        newPropertyMap.put( index, prop );
                    }
                }
            }
            this.propertyMap = newPropertyMap;
            propPhase = PropertyPhase.FULL_PROPERTY;
            return true;
        }
        else
        {
            if ( propertyMap == null )
            {
                propertyMap = new ArrayMap<Integer,Property>( 9, false, true );
            }
        }
        return false;
    }

    protected void setRollbackOnly()
    {
        nodeManager.setRollbackOnly();
    }
}