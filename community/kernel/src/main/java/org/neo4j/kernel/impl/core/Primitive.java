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

public abstract class Primitive
{
    // Used for marking that properties have been loaded but there just wasn't any.
    // Saves an extra trip down to the store layer.
    protected static final PropertyData[] NO_PROPERTIES = new PropertyData[0];

    protected abstract PropertyData changeProperty( NodeManager nodeManager, PropertyData property, Object value, TransactionState tx );

    protected abstract PropertyData addProperty( NodeManager nodeManager, Token index, Object value );

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

    // Force subclasses to implement equals
    @Override
    public abstract boolean equals(Object other);

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

        try
        {
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
                keys.add( nodeManager.getPropertyKeyToken( index ).name() );
            }
            if ( addMap != null )
            {
                for ( Integer index : addMap.keySet() )
                {
                    keys.add( nodeManager.getPropertyKeyToken( index ).name() );
                }
            }
        }
        catch ( TokenNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        return keys;
    }

    public Object getProperty( NodeManager nodeManager, String key ) throws NotFoundException
    {
        Token token = nodeManager.getPropertyKeyTokenOrNull( key );
        if ( token != null )
        {
            return getProperty( nodeManager, token.id() );
        }
        else
        {
            throw newPropertyNotFoundException( key );
        }
    }

    public Object getProperty( NodeManager nodeManager, int key ) throws NotFoundException
    {
        TransactionState tx = nodeManager.getTransactionState();
        ArrayMap<Integer,PropertyData> skipMap = null, addMap = null;
        if ( tx.hasChanges() )
        {
            skipMap = tx.getCowPropertyRemoveMap( this );
            addMap = tx.getCowPropertyAddMap( this );
        }

        ensureFullProperties( nodeManager );

        if ( skipMap != null && skipMap.get( key ) != null )
        {
            throw newPropertyNotFoundException( "No property with id " + key + " exists on " + this );
        }
        if ( addMap != null )
        {
            PropertyData property = addMap.get( key );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
        }
        PropertyData property = getPropertyForIndex(key );
        if ( property != null )
        {
            return getPropertyValue( nodeManager, property );
        }
        throw newPropertyNotFoundException( "No property with id " + key + " exists on " + this );
    }

    private NotFoundException newPropertyNotFoundException( String key )
    {
        return new NotFoundException( "'" + key + "' property not found for " + this + "." );
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
        Token index = nodeManager.getPropertyKeyTokenOrNull( key );
        if ( index != null )
        {
            if ( skipMap != null && skipMap.get( index.id() ) != null )
            {
                return defaultValue;
            }
            if ( addMap != null )
            {
                PropertyData property = addMap.get( index.id() );
                if ( property != null )
                {
                    return getPropertyValue( nodeManager, property );
                }
            }
            PropertyData property = getPropertyForIndex( index.id() );
            if ( property != null )
            {
                return getPropertyValue( nodeManager, property );
            }
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
        Token index = nodeManager.getPropertyKeyTokenOrNull( key );
        if ( index != null )
        {
            if ( skipMap != null && skipMap.get( index.id() ) != null )
            {
                return false;
            }
            if ( addMap != null )
            {
                PropertyData property = addMap.get( index.id() );
                if ( property != null )
                {
                    return true;
                }
            }
            PropertyData property = getPropertyForIndex( index.id() );
            if ( property != null )
            {
                return true;
            }
        }
        return false;
    }

    public Object setProperty( NodeManager nodeManager, PropertyContainer proxy, String key, Object value )
    {
        if ( key == null || value == null )
        {
            throw new IllegalArgumentException( "Null parameter, " + "key=" +
                key + ", " + "value=" + value );
        }
        TransactionState tx = nodeManager.getTransactionState();
        tx.acquireWriteLock( proxy );
        boolean success = false;
        Object previous = null;
        try
        {
            ensureFullProperties( nodeManager );
            ArrayMap<Integer,PropertyData> addMap = tx.getOrCreateCowPropertyAddMap( this );
            ArrayMap<Integer,PropertyData> skipMap = tx.getCowPropertyRemoveMap( this );
            Token index = null;
            PropertyData property = null;
            boolean foundInSkipMap = false;
            Token cachedIndex = nodeManager.getPropertyKeyTokenOrNull( key );
            if ( cachedIndex != null )
            {
                if ( skipMap != null && skipMap.remove( cachedIndex.id() ) != null )
                {
                    foundInSkipMap = true;
                }
                index = cachedIndex;
                property = addMap.get( cachedIndex.id() );
                if ( property == null )
                {
                    property = getPropertyForIndex( cachedIndex.id() );
                }
            }
            if ( index == null )
            {
                int keyId = nodeManager.getOrCreatePropertyKeyId( key );
                index = nodeManager.getPropertyKeyTokenOrNull( keyId );
            }
            if ( property != null && !foundInSkipMap )
            {
                previous = getPropertyValue( nodeManager, property );
                property = changeProperty( nodeManager, property, value, tx );
            }
            else
            {
                property = addProperty( nodeManager, index, value );
            }
            addMap.put( index.id(), property );
            success = true;
        }
        finally
        {
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
        return previous;
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
            PropertyData property = getAndUpdateTransactionStateForRemoveProperty( nodeManager, tx, key );
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
    
    private PropertyData getAndUpdateTransactionStateForRemoveProperty( NodeManager nodeManager, TransactionState tx,
            String key )
    {
        ArrayMap<Integer,PropertyData> addMap = tx.getCowPropertyAddMap( this );
        ArrayMap<Integer,PropertyData> removeMap = tx.getCowPropertyRemoveMap( this );
        Token cachedIndex = nodeManager.getPropertyKeyTokenOrNull( key );
        PropertyData property = null;
        if ( cachedIndex != null )
        {   // The property key exists
            if ( addMap != null )
            {   // There are transaction changes
                property = addMap.remove( cachedIndex.id() );
                if ( property != null )
                {   // This transaction has set this property on this node
                    removeMap = removeMap != null ? removeMap : tx.getOrCreateCowPropertyRemoveMap( this );
                    removeMap.put( cachedIndex.id(), property );
                    return property;
                }
            }
            if ( removeMap != null && removeMap.get( cachedIndex.id() ) != null )
            {
                return null;
            }
            property = getPropertyForIndex( cachedIndex.id() );
            if ( property != null )
            {
                removeMap = removeMap != null ? removeMap : tx.getOrCreateCowPropertyRemoveMap( this );
                removeMap.put( cachedIndex.id(), property );
            }
        }
        return property;
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

    protected List<PropertyEventData> getAllCommittedProperties( NodeManager nodeManager )
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
            Token index = nodeManager.getPropertyKeyTokenOrNull( property.getIndex() );
            Object value = getPropertyValue( nodeManager, property );
            props.add( new PropertyEventData( index.name(), value ) );
        }
        return props;
   }

    protected Object getCommittedPropertyValue( NodeManager nodeManager, String key )
    {
        ensureFullLightProperties( nodeManager );
        Token index = nodeManager.getPropertyKeyTokenOrNull( key );
        if ( index != null )
        {
            PropertyData property = getPropertyForIndex( index.id() );
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