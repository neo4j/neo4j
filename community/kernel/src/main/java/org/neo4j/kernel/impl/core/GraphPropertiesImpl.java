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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.DataStatement;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.api.properties.SafeProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.core.WritableTransactionState.CowEntityElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.api.properties.Property.property;

/**
 * A {@link PropertyContainer} (just like {@link Node} and {@link Relationship},
 * but instead holds properties associated with the graph itself rather than a
 * specific node or relationship. It uses a {@link Map} for caching the properties.
 * It's optimized for larger amounts of properties, but takes more memory than
 * an array based solution.
 */
public class GraphPropertiesImpl extends Primitive implements GraphProperties
{
    private final NodeManager nodeManager;
    private Map<Integer, SafeProperty> properties;
    private final ThreadToStatementContextBridge statementCtxProvider;

    GraphPropertiesImpl( NodeManager nodeManager, ThreadToStatementContextBridge statementCtxProvider )
    {
        super( false );
        this.nodeManager = nodeManager;
        this.statementCtxProvider = statementCtxProvider;
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return this.nodeManager.getGraphDbService();
    }

    @Override
    public int sizeOfObjectInBytesIncludingOverhead()
    {
        return 0;
    }

    @Override
    protected boolean hasLoadedProperties()
    {
        return properties != null;
    }

    @Override
    protected ArrayMap<Integer, PropertyData> loadProperties( NodeManager nodeManager )
    {
        return nodeManager.loadGraphProperties( false );
    }

    @Override
    protected Object loadPropertyValue( NodeManager nodeManager, int propertyKey )
    {
        return nodeManager.graphLoadPropertyValue( propertyKey );
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
            return false;

        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            long propertyId = statement.propertyKeyGetForName( key );
            if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                return false;
            }
            return statement.graphGetProperty( propertyId ).isDefined();
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            return false;
        }
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            long propertyId = statement.propertyKeyGetForName( key );
            if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                return false;
            }
            return statement.graphGetProperty( propertyId ).value();
        }
        catch ( PropertyKeyIdNotFoundException | PropertyNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            long propertyId = statement.propertyKeyGetForName( key );
            if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                return false;
            }
            return statement.graphGetProperty( propertyId ).value( defaultValue );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            return defaultValue;
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        boolean success = false;
        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            long propertyKeyId = statement.propertyKeyGetOrCreateForName( key );
            statement.graphSetProperty( property( propertyKeyId, value ) );
            success = true;
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Stefan/Jake", "A property key id disappeared under our feet" );
        }
        catch ( SchemaKernelException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
        finally
        {
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            long propertyId = statement.propertyKeyGetOrCreateForName( key );
            return statement.graphRemoveProperty( propertyId ).value( null );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Stefan/Jake", "A property key id disappeared under our feet" );
        }
        catch ( SchemaKernelException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            List<String> keys = new ArrayList<>();
            Iterator<SafeProperty> properties = statement.graphGetAllProperties();
            while ( properties.hasNext() )
            {
                keys.add( statement.propertyKeyGetName( properties.next().propertyKeyId() ) );
            }
            return keys;
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Jake", "Property key retrieved through kernel API should exist." );
        }
    }

    @Override
    public Iterable<Object> getPropertyValues()
    {
        try ( DataStatement statement = statementCtxProvider.dataStatement() )
        {
            return asSet( map( new Function<SafeProperty, Object>()
            {
                @Override
                public Object apply( SafeProperty prop )
                {
                    return prop.value();
                }
            }, statement.graphGetAllProperties() ) );
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof GraphProperties && ((GraphProperties) obj).getNodeManager().equals( nodeManager );
    }

    @Override
    public int hashCode()
    {
        return nodeManager.hashCode();
    }

    @Override
    public long getId()
    {
        return -1;
    }

    @Override
    protected void setEmptyProperties()
    {
        properties = new HashMap<>();
    }

    @Override
    protected Iterator<SafeProperty> getCachedProperties()
    {
        return properties.values().iterator();
    }

    @Override
    protected PrimitiveLongIterator getCachedPropertyKeys()
    {
        return new PropertyKeyIdIterator( getCachedProperties() );
    }

    @Override
    protected Property getCachedProperty( int key )
    {
        Property property = properties.get( key );
        return property != null ? property : Property.noGraphProperty( key );
    }

    @Override
    @SuppressWarnings("deprecation")
    protected PropertyData getPropertyForIndex( int keyId )
    {
        Property property = properties.get( keyId );
        return property != null ? property.asPropertyDataJustForIntegration() : null;
    }

    @Override
    protected void setProperties( Iterator<SafeProperty> loadedProperties )
    {
        if ( loadedProperties != null && loadedProperties.hasNext() )
        {
            Map<Integer, SafeProperty> newProperties = new HashMap<>();
            while ( loadedProperties.hasNext() )
            {
                SafeProperty property = loadedProperties.next();
                newProperties.put( (int) property.propertyKeyId(), property );
            }
            properties = newProperties;
        }
        else
        {
            properties = new HashMap<>();
        }
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

    @Override
    protected void commitPropertyMaps( ArrayMap<Integer, PropertyData> cowPropertyAddMap,
                                       ArrayMap<Integer, PropertyData> cowPropertyRemoveMap, long firstProp )
    {
        if ( cowPropertyAddMap != null ) for ( Map.Entry<Integer, PropertyData> entry : cowPropertyAddMap.entrySet() )
        {
            properties.put( entry.getKey(), Property.property( entry.getKey(), entry.getValue().getValue() ) );
        }
        if ( cowPropertyRemoveMap != null ) for ( Map.Entry<Integer, PropertyData> entry : cowPropertyRemoveMap.entrySet() )
        {
            properties.remove( entry.getKey() );
        }
    }
}
