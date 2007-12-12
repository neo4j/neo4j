/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayIntSet;
import org.neo4j.impl.util.ArrayMap;

abstract class NeoPrimitive
{
    private static enum PropertyPhase 
    { 
        EMPTY_PROPERTY, 
        FULL_PROPERTY,
    }
    
    private PropertyPhase propPhase;

    private ArrayMap<Integer,Property> propertyMap = null; 
    
    protected final NodeManager nodeManager; 

    protected abstract void changeProperty( int propertyId, Object value );
    protected abstract int addProperty( PropertyIndex index, Object value );
    protected abstract void removeProperty( int propertyId );
    protected abstract RawPropertyData[] loadProperties();
    
    
    NeoPrimitive( NodeManager nodeManager )
    {
        this.nodeManager = nodeManager;
        this.propPhase = PropertyPhase.EMPTY_PROPERTY;
    }
    
    NeoPrimitive( boolean newPrimitive, NodeManager nodeManager )
    {
        this.nodeManager = nodeManager;
        if ( newPrimitive )
        {
            this.propPhase = PropertyPhase.FULL_PROPERTY;
        }
    }
        
    public Iterable<Object> getPropertyValues()
    {
        nodeManager.acquireLock( this, LockType.READ );
        try
        {
            ensureFullProperties();
            List<Object> properties = new ArrayList<Object>();
            for ( Property property : propertyMap.values() )
            {
                properties.add( getPropertyValue( property ) );
            }
            return properties;
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.READ );
        }
    }
    
    public Iterable<String> getPropertyKeys()
    {
        nodeManager.acquireLock( this, LockType.READ );
        try
        {
            ensureFullProperties();
            List<String> propertyKeys = new ArrayList<String>();
            for ( int keyId : propertyMap.keySet() )
            {
                propertyKeys.add( nodeManager.getIndexFor( keyId ).getKey() );
            }
            return propertyKeys;
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.READ );
        }           
    }

    public Object getProperty( String key ) 
        throws NotFoundException
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key" );
        }
        nodeManager.acquireLock( this, LockType.READ );
        try
        {
            for ( PropertyIndex index : nodeManager.index( key ) )
            {
                Property property = null;
                if ( propertyMap != null )
                {
                    property = propertyMap.get( index.getKeyId() );
                }
                if ( property != null )
                {
                    return getPropertyValue( property );
                }
                
                if ( ensureFullProperties() )
                {
                    property = propertyMap.get( index.getKeyId() );
                    if ( property != null )
                    {
                        return getPropertyValue( property );
                    }
                }
            }
            if ( !nodeManager.hasAllPropertyIndexes() )
            {
                ensureFullProperties();
                for ( int keyId : propertyMap.keySet() )
                {
                    if ( !nodeManager.hasIndexFor( keyId ) )
                    {
                        PropertyIndex indexToCheck = nodeManager.getIndexFor( 
                            keyId );
                        if ( indexToCheck.getKey().equals( key ) )
                        {
                            Property property = propertyMap.get( 
                                indexToCheck.getKeyId() );
                            return getPropertyValue( property );
                        }
                    }
                }
            }
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.READ );
        }
        throw new NotFoundException( "" + key + " property not found." );
    }
    
    public Object getProperty( String key, Object defaultValue )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "null key" );
        }
        nodeManager.acquireLock( this, LockType.READ );
        try
        {
            for ( PropertyIndex index : nodeManager.index( key ) )
            {
                Property property = null;
                if ( propertyMap != null ) 
                {
                    property = propertyMap.get( index.getKeyId() );
                }
                if ( property != null )
                {
                    return getPropertyValue( property );
                }
                
                if ( ensureFullProperties() )
                {
                    property = propertyMap.get( index.getKeyId() );
                    if ( property != null )
                    {
                        return getPropertyValue( property );
                    }
                }
            }
            if ( !nodeManager.hasAllPropertyIndexes() )
            {
                ensureFullProperties();
                for ( int keyId : propertyMap.keySet() )
                {
                    if ( !nodeManager.hasIndexFor( keyId ) )
                    {
                        PropertyIndex indexToCheck = nodeManager.getIndexFor( 
                            keyId );
                        if ( indexToCheck.getKey().equals( key ) )
                        {
                            Property property = propertyMap.get( 
                                indexToCheck.getKeyId() );
                            return getPropertyValue( property );
                        }
                    }
                }
            }
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.READ );
        }
        return defaultValue;
    }

    public boolean hasProperty( String key )
    {
        nodeManager.acquireLock( this, LockType.READ );
        try
        {
            for ( PropertyIndex index : nodeManager.index( key ) )
            {
                Property property = null;
                if ( propertyMap != null )
                {
                    property = propertyMap.get( index.getKeyId() );
                }
                if ( property != null )
                {
                    return true;
                }
                if ( ensureFullProperties() )
                {
                    if ( propertyMap.get( index.getKeyId() ) != null )
                    {
                        return true;
                    }
                }
            }
            ensureFullProperties();
            for ( int keyId : propertyMap.keySet() )
            {
                PropertyIndex indexToCheck = nodeManager.getIndexFor( keyId );
                if ( indexToCheck.getKey().equals( key ) )
                {
                    return true;
                }
            }
            return false;
        }
        finally
        {
            nodeManager.releaseLock( this, LockType.READ );
        }           
    }
    
    public void setProperty( String key, Object value ) 
    {
        if ( key == null || value == null )
        {
            throw new IllegalArgumentException( "Null parameter, " +
                "key=" + key + ", " + "value=" + value );
        }
        nodeManager.acquireLock( this, LockType.WRITE );
        boolean success = false;
        try
        {
            // must make sure we don't add already existing property
            ensureFullProperties();
            PropertyIndex index = null;
            Property property = null;
            for ( PropertyIndex cachedIndex : nodeManager.index( key ) )
            {
                property = propertyMap.get( cachedIndex.getKeyId() );
                index = cachedIndex;
                if ( property != null )
                {
                    break;
                }
            }
            if ( property == null && !nodeManager.hasAllPropertyIndexes() )
            {
                for ( int keyId : propertyMap.keySet() )
                {
                    if ( !nodeManager.hasIndexFor( keyId ) )
                    {
                        PropertyIndex indexToCheck = nodeManager.getIndexFor( 
                            keyId );
                        if ( indexToCheck.getKey().equals( key ) )
                        {
                            index = indexToCheck;
                            property = propertyMap.get( 
                                indexToCheck.getKeyId() );
                            break;
                        }
                    }
                }
            }
            if ( index == null )
            {
                index = nodeManager.createPropertyIndex( key );
            }
            if ( property != null )
            {
                int propertyId = property.getId();
                changeProperty( propertyId, value );
                property.setNewValue( value );
            }
            else
            {
                int propertyId = addProperty( index, value );
                propertyMap.put( index.getKeyId(), 
                    new Property( propertyId, value ) );
            }
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
            Property property = null;
            for ( PropertyIndex cachedIndex : nodeManager.index( key ) )
            {
                if ( propertyMap != null )
                {
                    property = propertyMap.remove( cachedIndex.getKeyId() );
                }
                if ( property == null )
                {
                    if ( ensureFullProperties() )
                    {
                        property = propertyMap.remove( cachedIndex.getKeyId() );
                        if ( property != null )
                        {
                            break;
                        }
                    }
                }
                else
                {
                    break;
                }
            }
            if ( property == null && !nodeManager.hasAllPropertyIndexes() )
            {
                ensureFullProperties();
                for ( int keyId : propertyMap.keySet() )
                {
                    if ( !nodeManager.hasIndexFor( keyId ) )
                    {
                        PropertyIndex indexToCheck = nodeManager.getIndexFor( 
                            keyId );
                        if ( indexToCheck.getKey().equals( key ) )
                        {
                            property = propertyMap.remove( 
                                indexToCheck.getKeyId() );
                            break;
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
                Property property = new Property( propId, 
                    propData.getValue() );
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
                propertyMap = 
                    new ArrayMap<Integer,Property>( 9, false, true );
            }
        }
        return false;
    }
    
    protected void setRollbackOnly()
    {
        nodeManager.setRollbackOnly();
    }
}