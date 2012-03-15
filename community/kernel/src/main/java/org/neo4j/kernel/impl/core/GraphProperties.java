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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.core.LockReleaser.CowEntityElement;
import org.neo4j.kernel.impl.core.LockReleaser.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

/**
 * A {@link PropertyContainer} (just like {@link Node} and {@link Relationship},
 * but instead holds properties associated with the graph itself rather than a
 * specific node or relationship. It uses a {@link Map} for caching the properties.
 * It's optimized for larger amounts of properties, but takes more memory than
 * an array based solution.
 */
public class GraphProperties extends Primitive implements PropertyContainer
{
    private final NodeManager nodeManager;
    
    private Map<Integer, PropertyData> properties;
    private boolean loaded;
    
    GraphProperties( NodeManager nodeManager )
    {
        super( false );
        this.nodeManager = nodeManager;
    }
    
    public int size()
    {
        // only one instance of this and will never go into cache
        throw new UnsupportedOperationException();
    }

    @Override
    protected void updateSize( int before, int after, NodeManager nodeManager )
    {
        // only one instance of this and will never go into cache
        throw new UnsupportedOperationException();
    }
    
    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return this.nodeManager.getGraphDbService();
    }

    @Override
    protected PropertyData changeProperty( NodeManager nodeManager, PropertyData property, Object value )
    {
        return nodeManager.graphChangeProperty( property, value );
    }

    @Override
    protected PropertyData addProperty( NodeManager nodeManager, PropertyIndex index, Object value )
    {
        return nodeManager.graphAddProperty( index, value );
    }

    @Override
    protected void removeProperty( NodeManager nodeManager, PropertyData property )
    {
        nodeManager.graphRemoveProperty( property );
    }

    @Override
    protected ArrayMap<Integer, PropertyData> loadProperties( NodeManager nodeManager, boolean light )
    {
        return nodeManager.loadGraphProperties( light );
    }

    @Override
    public long getId()
    {
        return -1L;
    }

    @Override
    public boolean hasProperty( String key )
    {
        return hasProperty( nodeManager, key );
    }

    @Override
    public Object getProperty( String key )
    {
        return getProperty( nodeManager, key );
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        return getProperty( nodeManager, key, defaultValue );
    }

    @Override
    public void setProperty( String key, Object value )
    {
        setProperty( nodeManager, this, key, value );
    }

    @Override
    public Object removeProperty( String key )
    {
        return removeProperty( nodeManager, this, key );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return getPropertyKeys( nodeManager );
    }

    @Override
    public Iterable<Object> getPropertyValues()
    {
        return getPropertyValues( nodeManager );
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
    
    @Override
    protected void commitPropertyMaps( ArrayMap<Integer, PropertyData> cowPropertyAddMap,
            ArrayMap<Integer, PropertyData> cowPropertyRemoveMap, long firstProp )
    {
        if ( cowPropertyAddMap != null ) for ( Map.Entry<Integer, PropertyData> property : cowPropertyAddMap.entrySet() )
        {
            properties.put( property.getKey(), property.getValue() );
        }
        if ( cowPropertyRemoveMap != null ) for ( Map.Entry<Integer, PropertyData> property : cowPropertyRemoveMap.entrySet() )
        {
            properties.remove( property.getKey() );
        }
    }
    
    @Override
    protected void setEmptyProperties()
    {
        properties = new HashMap<Integer, PropertyData>();
        loaded = true;
    }

    @Override
    protected PropertyData[] allProperties()
    {
        return !loaded ? null : properties.values().toArray( new PropertyData[properties.size()] );
    }

    @Override
    protected PropertyData getPropertyForIndex( int keyId )
    {
        return properties.get( keyId );
    }

    @Override
    protected void setProperties( ArrayMap<Integer, PropertyData> loadedProperties, NodeManager nodeManager )
    {
        if ( loadedProperties != null && loadedProperties.size() > 0 )
        {
            Map<Integer, PropertyData> newProperties = new HashMap<Integer, PropertyData>();
            for ( Map.Entry<Integer, PropertyData> property : loadedProperties.entrySet() )
            {
                newProperties.put( property.getKey(), property.getValue() );
            }
            properties = newProperties;
        }
        else
        {
            properties = new HashMap<Integer, PropertyData>();
        }
        loaded = true;
    }
    
    @Override
    public CowEntityElement getEntityElement( PrimitiveElement element, boolean create )
    {
        return element.graphElement( create );
    }
    
    @Override
    PropertyContainer asProxy( NodeManager nm )
    {
        return this;
    }
    
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof GraphProperties ) ) return false;
        return ((GraphProperties)obj).nodeManager.equals( nodeManager );
    }
    
    @Override
    public int hashCode()
    {
        return nodeManager.hashCode();
    }
}
