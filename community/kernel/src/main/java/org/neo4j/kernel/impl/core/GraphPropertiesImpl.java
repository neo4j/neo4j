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
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
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
    private Map<Integer, Property> properties;
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

        StatementOperationParts ctxForReading = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyId = ctxForReading.keyReadOperations().propertyKeyGetForName( state, key );
            return ctxForReading.entityReadOperations().graphHasProperty( state, propertyId );
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
            state.close();
        }
    }

    @Override
    public Object getProperty( String key )
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementOperationParts ctxForReading = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyId = ctxForReading.keyReadOperations().propertyKeyGetForName( state, key );
            return ctxForReading.entityReadOperations().graphGetProperty( state, propertyId ).value();
        }
        catch ( PropertyKeyIdNotFoundException | PropertyKeyNotFoundException | PropertyNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        // TODO: Push this check to getPropertyKeyId
        // ^^^^^ actually, if the key is null, we could fail before getting the statement context...
        if ( null == key )
            throw new IllegalArgumentException( "(null) property key is not allowed" );

        StatementOperationParts ctxForReading = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
        try
        {
            long propertyId = ctxForReading.keyReadOperations().propertyKeyGetForName( state, key );
            return ctxForReading.entityReadOperations().graphGetProperty( state, propertyId ).value(defaultValue);
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
            state.close();
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        StatementOperationParts ctxForWriting = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        boolean success = false;
        try
        {
            long propertyKeyId = ctxForWriting.keyWriteOperations().propertyKeyGetOrCreateForName( state, key );
            ctxForWriting.entityWriteOperations().graphSetProperty( state, property( propertyKeyId, value ) );
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
            state.close();
            if ( !success )
            {
                nodeManager.setRollbackOnly();
            }
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        StatementOperationParts ctxForWriting = statementCtxProvider.getCtxForWriting();
        StatementState state = statementCtxProvider.statementForWriting();
        try
        {
            long propertyId = ctxForWriting.keyWriteOperations().propertyKeyGetOrCreateForName( state, key );
            return ctxForWriting.entityWriteOperations().graphRemoveProperty( state, propertyId ).value( null );
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
            state.close();
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        final StatementOperationParts context = statementCtxProvider.getCtxForReading();
        final StatementState state = statementCtxProvider.statementForReading();
        try
        {
            List<String> keys = new ArrayList<>();
            PrimitiveLongIterator keyIds = context.entityReadOperations().graphGetPropertyKeys( state );
            while ( keyIds.hasNext() )
            {
                keys.add( context.keyReadOperations().propertyKeyGetName( state, keyIds.next() ) );
            }
            return keys;
        }
        catch ( PropertyKeyIdNotFoundException e )
        {
            throw new ThisShouldNotHappenError( "Jake",
                    "Property key retrieved through kernel API should exist." );
        }
        finally
        {
            state.close();
        }
    }

    @Override
    public Iterable<Object> getPropertyValues()
    {
        final StatementOperationParts context = statementCtxProvider.getCtxForReading();
        StatementState state = statementCtxProvider.statementForReading();
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
            }, context.entityReadOperations().graphGetAllProperties( state ) ) );
        }
        finally
        {
            state.close();
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
    protected Iterator<Property> getCachedProperties()
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
    protected void setProperties( Iterator<Property> loadedProperties )
    {
        if ( loadedProperties != null && loadedProperties.hasNext() )
        {
            Map<Integer, Property> newProperties = new HashMap<>();
            while ( loadedProperties.hasNext() )
            {
                Property property = loadedProperties.next();
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
