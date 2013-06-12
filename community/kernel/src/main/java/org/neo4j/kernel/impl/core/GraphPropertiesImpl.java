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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.properties.Property;
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
    private Map<Integer, Property> properties;
    private boolean loaded;
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

        StatementContext ctxForReading = statementCtxProvider.getCtxForReading();
        try
        {
            long propertyId = ctxForReading.propertyKeyGetForName( key );
            return ctxForReading.graphHasProperty( propertyId );
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            return false;
        }
        catch ( PropertyKeyNotFoundException e )
        {
            return false;
        }
        finally
        {
            ctxForReading.close();
        }
    }

    @Override
    public Object getProperty( String key )
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementContext ctxForReading = statementCtxProvider.getCtxForReading();
        try
        {
            long propertyId = ctxForReading.propertyKeyGetForName( key );
            return ctxForReading.graphGetProperty( propertyId ).value();
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( PropertyNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        finally
        {
            ctxForReading.close();
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementContext ctxForReading = statementCtxProvider.getCtxForReading();
        try
        {
            long propertyId = ctxForReading.propertyKeyGetForName( key );
            return ctxForReading.graphGetProperty( propertyId ).value(defaultValue);
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            return defaultValue;
        }
        catch ( PropertyKeyNotFoundException e )
        {
            return defaultValue;
        }
        finally
        {
            ctxForReading.close();
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        StatementContext ctxForWriting = statementCtxProvider.getCtxForWriting();
        boolean success = false;
        try
        {
            long propertyKeyId = ctxForWriting.propertyKeyGetOrCreateForName( key );
            ctxForWriting.graphSetProperty( property( propertyKeyId, value ) );
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
            ctxForWriting.close();
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        StatementContext ctxForWriting = statementCtxProvider.getCtxForWriting();
        try
        {
            long propertyId = ctxForWriting.propertyKeyGetOrCreateForName( key );
            return ctxForWriting.graphRemoveProperty( propertyId ).value( null );
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
            ctxForWriting.close();
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        final StatementContext context = statementCtxProvider.getCtxForReading();
        try
        {
            return asSet( map( new Function<Long, String>() {
                @Override
                public String apply( Long propertyKeyId )
                {
                    try
                    {
                        return context.propertyKeyGetName( propertyKeyId );
                    }
                    catch ( PropertyKeyIdNotFoundException e )
                    {
                        throw new ThisShouldNotHappenError( "Jake",
                                "Property key retrieved through kernel API should exist." );
                    }
                }
            }, context.graphGetPropertyKeys() ) );
        }
        finally
        {
            context.close();
        }

    }

    @Override
    public Iterable<Object> getPropertyValues()
    {
        final StatementContext context = statementCtxProvider.getCtxForReading();
        try
        {
            return asSet( map( new Function<Property,Object>() {
                @Override
                public Object apply( Property prop )
                {
                    try
                    {
                        return prop.value();
                    }
                    catch ( PropertyNotFoundException e )
                    {
                        throw new ThisShouldNotHappenError( "Jake",
                                "Property key retrieved through kernel API should exist." );
                    }
                }
            }, context.graphGetAllProperties() ) );
        }
        finally
        {
            context.close();
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
        if ( !(obj instanceof GraphProperties) )
            return false;
        return ((GraphProperties) obj).getNodeManager().equals( nodeManager );
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
        properties = new HashMap<Integer, Property>();
        loaded = true;
    }
    
    @Override
    protected Iterator<Property> getCachedProperties()
    {
        return properties.values().iterator();
    }

    @Override
    protected PropertyData getPropertyForIndex( int keyId )
    {
        Property property = properties.get( keyId );
        return property != null ? property.asPropertyDataJustForIntegration() : null;
    }

    @Override
    protected void setProperties( Iterator<Property> loadedProperties )
    {
        if ( loadedProperties != null && loadedProperties.hasNext() )
        {
            Map<Integer, Property> newProperties = new HashMap<Integer, Property>();
            while ( loadedProperties.hasNext() )
            {
                Property property = loadedProperties.next();
                newProperties.put( (int) property.propertyKeyId(), property );
            }
            properties = newProperties;
        }
        else
        {
            properties = new HashMap<Integer, Property>();
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
