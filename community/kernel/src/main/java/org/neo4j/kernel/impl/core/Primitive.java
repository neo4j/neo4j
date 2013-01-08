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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.impl.core.WritableTransactionState.CowEntityElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

abstract class Primitive
{
    // Used for marking that properties have been loaded but there just wasn't any.
    // Saves an extra trip down to the store layer.
    protected static final PropertyData[] NO_PROPERTIES = new PropertyData[0];

    protected abstract PropertyData changeProperty( NodeManager nodeManager, PropertyData property, Object value, TransactionState tx );

    protected abstract PropertyData addProperty( NodeManager nodeManager, PropertyIndex index, Object value );

    protected abstract void removeProperty( NodeManager nodeManager,
            PropertyData property, TransactionState tx );

    protected abstract ArrayMap<Integer, PropertyData> loadProperties(
            NodeManager nodeManager, boolean light );

    Primitive( boolean newPrimitive )
    {
        if ( newPrimitive ) setEmptyProperties();
    }

    public abstract long getId();
    
    protected abstract void setEmptyProperties();
    
    protected abstract PropertyData[] allProperties();

    protected abstract PropertyData getPropertyForIndex( int keyId );
    
    protected abstract void setProperties( ArrayMap<Integer, PropertyData> properties, NodeManager nodeManager );
    
    protected abstract void commitPropertyMaps(
            ArrayMap<Integer,PropertyData> cowPropertyAddMap,
            ArrayMap<Integer,PropertyData> cowPropertyRemoveMap, long firstProp, NodeManager nodeManager );

    @Override
    public int hashCode()
    {
        long id = getId();
        return (int) (( id >>> 32 ) ^ id );
    }

    public Iterable<Object> getPropertyValues( NodeManager nodeManager )
    {
        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer,PropertyData> skipMap = null, addMap = null;
        if ( tx.hasChanges() )
        {
            skipMap = tx.getCowPropertyRemoveMap( this );
            addMap = tx.getCowPropertyAddMap( this );
        }

        ensureFullProperties( nodeManager );
        List<Object> values = new ArrayList<Object>();

        for ( PropertyData property : allProperties() )
        {
            Integer index = property.getIndex();
            if ( skipMap != null && skipMap.get( index ) != null )
            {
                continue;
            }
            if ( addMap != null && addMap.get( index ) != null )
            {
                continue;
            }
            values.add( getPropertyValue( nodeManager, property ) );
        }
        if ( addMap != null )
        {
            for ( PropertyData property : addMap.values() )
            {
                values.add( getPropertyValue( nodeManager, property ) );
            }
        }
        return values;
    }

    public Iterable<String> getPropertyKeys( NodeManager nodeManager )
    {
        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer,PropertyData> skipMap = null, addMap = null;
        if ( tx.hasChanges() )
        {
            skipMap = tx.getCowPropertyRemoveMap( this );
            addMap = tx.getCowPropertyAddMap( this );
        }

        ensureFullProperties( nodeManager );
        List<String> keys = new ArrayList<String>();

        for ( PropertyData property : allProperties() )
        {
            Integer index = property.getIndex();
            if ( skipMap != null && skipMap.get( index ) != null )
            {
                continue;
            }
            if ( addMap != null && addMap.get( index ) != null )
            {
                continue;
            }
            keys.add( nodeManager.getIndexFor( index, tx ).getKey() );
        }
        if ( addMap != null )
        {
            for ( Integer index : addMap.keySet() )
            {
                keys.add( nodeManager.getIndexFor( index, tx ).getKey() );
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
        
        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer,PropertyData> skipMap = null, addMap = null;
        if ( tx.hasChanges() )
        {
            skipMap = tx.getCowPropertyRemoveMap( this );
            addMap = tx.getCowPropertyAddMap( this );
        }

        ensureFullProperties( nodeManager );
        for ( PropertyIndex index : nodeManager.index( key, tx ) )
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
            PropertyData property = getPropertyForIndex( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
        }
        PropertyData property = getSlowProperty( nodeManager, addMap, skipMap, key, tx );
        if ( property != null )
        {
            return getPropertyValue( nodeManager, property );
        }
        throw newPropertyNotFoundException( key );
    }

    private NotFoundException newPropertyNotFoundException( String key )
    {
        return new NotFoundException( "'" + key + "' property not found for " + this + "." );
    }

    private PropertyData getSlowProperty( NodeManager nodeManager,
            ArrayMap<Integer, PropertyData> addMap,
            ArrayMap<Integer,PropertyData> skipMap, String key, TransactionState tx )
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
                        nodeManager.getIndexFor( keyId, tx );
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
        for ( PropertyData property : allProperties() )
        {
            int keyId = property.getIndex();
            if ( !nodeManager.hasIndexFor( keyId ) )
            {
                PropertyIndex indexToCheck = nodeManager.getIndexFor( keyId, tx );
                if ( indexToCheck.getKey().equals( key ) )
                {
                    if ( skipMap != null && skipMap.get( keyId ) != null )
                    {
                        throw newPropertyNotFoundException( key );
                    }
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

        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer,PropertyData> skipMap = null, addMap = null;
        if ( tx.hasChanges() )
        {
            skipMap = tx.getCowPropertyRemoveMap( this );
            addMap = tx.getCowPropertyAddMap( this );
        }

        ensureFullProperties( nodeManager );
        for ( PropertyIndex index : nodeManager.index( key, tx ) )
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
            PropertyData property = getPropertyForIndex( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
        }
        PropertyData property = getSlowProperty( nodeManager, addMap, skipMap, key, tx );
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

        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer,PropertyData> skipMap = null, addMap = null;
        if ( tx.hasChanges() )
        {
            skipMap = tx.getCowPropertyRemoveMap( this );
            addMap = tx.getCowPropertyAddMap( this );
        }

        ensureFullProperties( nodeManager );
        for ( PropertyIndex index : nodeManager.index( key, tx ) )
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
            PropertyData property = getPropertyForIndex( index.getKeyId() );
            if ( property != null )
            {
                return true;
            }
        }
        PropertyData property = getSlowProperty( nodeManager, addMap, skipMap, key, tx );
        if ( property != null )
        {
            return true;
        }
        return false;
    }

    public void setProperty( NodeManager nodeManager, PropertyContainer proxy, String key, Object value )
    {
        if ( key == null || value == null )
        {
            throw new IllegalArgumentException( "Null parameter, " + "key=" +
                key + ", " + "value=" + value );
        }
        TransactionState tx = nodeManager.getTransactionState();
        tx.acquireWriteLock( proxy );
        boolean success = false;
        try
        {
            ensureFullProperties( nodeManager );
            ArrayMap<Integer,PropertyData> addMap = tx.getOrCreateCowPropertyAddMap( this );
            ArrayMap<Integer,PropertyData> skipMap = tx.getCowPropertyRemoveMap( this );
            PropertyIndex index = null;
            PropertyData property = null;
            boolean foundInSkipMap = false;
            for ( PropertyIndex cachedIndex : nodeManager.index( key, tx ) )
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
                property = getPropertyForIndex( cachedIndex.getKeyId() );
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
                            .getIndexFor( keyId, tx );
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
                    for ( PropertyData aProperty : allProperties() )
                    {
                        int keyId = aProperty.getIndex();
                        if ( !nodeManager.hasIndexFor( keyId ) )
                        {
                            PropertyIndex indexToCheck = nodeManager
                                .getIndexFor( keyId, tx );
                            if ( indexToCheck.getKey().equals( key ) )
                            {
                                if ( skipMap != null )
                                {
                                    skipMap.remove( indexToCheck.getKeyId() );
                                }
                                index = indexToCheck;
                                property = getPropertyForIndex( indexToCheck.getKeyId() );
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
                property = changeProperty( nodeManager, property, value, tx );
            }
            else
            {
                property = addProperty( nodeManager, index, value );
            }
            if ( addMap == null ) System.out.println( "addMap null" );
            if ( index == null ) System.out.println( "index null" );
            addMap.put( index.getKeyId(), property );
            success = true;
        }
        finally
        {
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    public Object removeProperty( NodeManager nodeManager, PropertyContainer proxy, String key )
    {
        if ( key == null )
        {
            throw new IllegalArgumentException( "Null parameter." );
        }
        boolean success = false;
        TransactionState tx = nodeManager.getTransactionState();
        tx.acquireWriteLock( proxy );
        try
        {
            ensureFullProperties( nodeManager );
            PropertyData property = null;
            ArrayMap<Integer,PropertyData> addMap = tx.getCowPropertyAddMap( this );

            // Don't create the map if it doesn't exist here... but instead when (and if)
            // the property is found below.
            ArrayMap<Integer,PropertyData> removeMap = tx.getCowPropertyRemoveMap( this );
            for ( PropertyIndex cachedIndex : nodeManager.index( key, tx ) )
            {
                if ( addMap != null )
                {
                    property = addMap.remove( cachedIndex.getKeyId() );
                    if ( property != null )
                    {
                        removeMap = removeMap != null ? removeMap : tx.getOrCreateCowPropertyRemoveMap( this );
                        removeMap.put( cachedIndex.getKeyId(), property );
                        break;
                    }
                }
                if ( removeMap != null && removeMap.get( cachedIndex.getKeyId() ) != null )
                {
                    success = true;
                    return null;
                }
                property = getPropertyForIndex( cachedIndex.getKeyId() );
                if ( property != null )
                {
                    removeMap = removeMap != null ? removeMap : tx.getOrCreateCowPropertyRemoveMap( this );
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
                                .getIndexFor( keyId, tx );
                            if ( indexToCheck.getKey().equals( key ) )
                            {
                                property = addMap.remove( indexToCheck
                                    .getKeyId() );
                                if ( property != null )
                                {
                                    removeMap = removeMap != null ? removeMap : tx.getOrCreateCowPropertyRemoveMap( this );
                                    removeMap.put( indexToCheck.getKeyId(),
                                        property );
                                    break;
                                }
                            }
                        }
                    }
                    if ( property == null )
                    {
                        for ( PropertyData aProperty : allProperties() )
                        {
                            int keyId = aProperty.getIndex();
                            if ( !nodeManager.hasIndexFor( keyId ) )
                            {
                                PropertyIndex indexToCheck = nodeManager
                                    .getIndexFor( keyId, tx );
                                if ( indexToCheck.getKey().equals( key ) )
                                {
                                    property = getPropertyForIndex( indexToCheck.getKeyId() );
                                    if ( property != null )
                                    {
                                        removeMap = removeMap != null ? removeMap : tx.getOrCreateCowPropertyRemoveMap( this );
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
            removeProperty( nodeManager, property, tx );
            success = true;
            return getPropertyValue( nodeManager, property );
        }
        finally
        {
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
            /*
             * This will only happen for "heavy" property value, such as
             * strings/arrays
             */
            value = nodeManager.loadPropertyValue( property );
            property.setNewValue( value );
        }
        return value;
    }

    private void ensureFullProperties( NodeManager nodeManager )
    {
       ensureFullProperties( nodeManager, /* light = */ false );
    }

    private void ensureFullLightProperties( NodeManager nodeManager )
    {
       ensureFullProperties( nodeManager, /* light = */ true );
    }

    private void ensureFullProperties(NodeManager nodeManager, boolean light )
    {
        // double checked locking
        if ( allProperties() == null ) synchronized ( this )
        {
            if ( allProperties() == null )
            {
                try
                {
                    ArrayMap<Integer, PropertyData> loadedProperties = loadProperties( nodeManager, light );
                    setProperties( loadedProperties, nodeManager );
                }
                catch ( InvalidRecordException e )
                {
                    throw new NotFoundException( asProxy( nodeManager ) + " not found. This can be because someone " +
                            "else deleted this entity while we were trying to read properties from it, or because of " +
                            "concurrent modification of other properties on this entity. The problem should be temporary.", e );
                }
            }
        }
    }

    protected List<PropertyEventData> getAllCommittedProperties( NodeManager nodeManager, TransactionState tx )
    {
        ensureFullLightProperties( nodeManager );
        if ( allProperties() == null )
        {
            return new ArrayList<PropertyEventData>();
        }
        PropertyData[] properties = allProperties();
        List<PropertyEventData> props =
            new ArrayList<PropertyEventData>( properties.length );
        for ( PropertyData property : properties )
        {
            PropertyIndex index = nodeManager.getIndexFor( property.getIndex(), tx );
            Object value = getPropertyValue( nodeManager, property );
            props.add( new PropertyEventData( index.getKey(), value ) );
        }
        return props;
   }

    protected Object getCommittedPropertyValue( NodeManager nodeManager, String key, TransactionState tx )
    {
        ensureFullLightProperties( nodeManager );
        for ( PropertyIndex index : nodeManager.index( key, tx ) )
        {
            PropertyData property = getPropertyForIndex( index.getKeyId() );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
        }
        return null;
    }
    
    public abstract CowEntityElement getEntityElement( PrimitiveElement element, boolean create );
    
    abstract PropertyContainer asProxy( NodeManager nm );
}