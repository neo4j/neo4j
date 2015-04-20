/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;

import static java.util.Arrays.sort;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
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
    private final long epoch;
    private final ThreadToStatementContextBridge statementContextProvider;
    private Map<Integer,DefinedProperty> properties;

    GraphPropertiesImpl( long epoch, ThreadToStatementContextBridge statementContextProvider )
    {
        this.epoch = epoch;
        this.statementContextProvider = statementContextProvider;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
//        return this.nodeManager.getGraphDbService();
        throw new UnsupportedOperationException( "Please implement" );
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
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyId = statement.readOperations().propertyKeyGetForName( key );
            return statement.readOperations().graphGetProperty( propertyId ).isDefined();
        }
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = statementContextProvider.instance() )
        {
            try
            {
                int propertyId = statement.readOperations().propertyKeyGetForName( key );
                if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
                {
                    throw new NotFoundException( String.format( "No such property, '%s'.", key ) );
                }
                return statement.readOperations().graphGetProperty( propertyId ).value();
            }
            catch ( PropertyNotFoundException e )
            {
                throw new NotFoundException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyId = statement.readOperations().propertyKeyGetForName( key );
            if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                return false;
            }
            return statement.readOperations().graphGetProperty( propertyId ).value( defaultValue );
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            statement.dataWriteOperations().graphSetProperty( property( propertyKeyId, value ) );
        }
        catch ( IllegalTokenNameException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            int propertyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            return statement.dataWriteOperations().graphRemoveProperty( propertyId ).value( null );
        }
        catch ( IllegalTokenNameException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( Statement statement = statementContextProvider.instance() )
        {
            List<String> keys = new ArrayList<>();
            Iterator<DefinedProperty> properties = statement.readOperations().graphGetAllProperties();
            while ( properties.hasNext() )
            {
                keys.add( statement.readOperations().propertyKeyGetName( properties.next().propertyKeyId() ) );
            }
            return keys;
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Jake", "Property key retrieved through kernel API should exist." );
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
        return obj instanceof GraphPropertiesImpl && ((GraphPropertiesImpl) obj).epoch == epoch;
    }

    @Override
    public int hashCode()
    {
        return Long.valueOf( epoch ).hashCode();
    }

    @Override
    public long getId()
    {
        return -1;
    }

    @Override
    protected Iterator<DefinedProperty> getCachedProperties()
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
    protected void setProperties(
            Iterator<DefinedProperty> loadedProperties )
    {
        if ( loadedProperties != null && loadedProperties.hasNext() )
        {
            Collection<DefinedProperty> propertiesCollection = asCollection( loadedProperties );
            DefinedProperty[] propertiesArray = propertiesCollection.toArray(
                    new DefinedProperty[propertiesCollection.size()] );
            sort( propertiesArray, ArrayBasedPrimitive.PROPERTY_DATA_COMPARATOR_FOR_SORTING );

            Map<Integer,DefinedProperty> newProperties = new HashMap<>();
            for ( DefinedProperty property : propertiesArray )
            {
                newProperties.put( property.propertyKeyId(), property );
            }
            properties = newProperties;
        }
        else
        {
            properties = new HashMap<>();
        }
    }

    @Override
    public void commitPropertyMaps( PrimitiveIntObjectMap<DefinedProperty> cowPropertyAddMap,
                                    Iterator<Integer> removed )
    {
        if ( cowPropertyAddMap != null )
        {
            PrimitiveIntIterator keyIterator = cowPropertyAddMap.iterator();
            while ( keyIterator.hasNext() )
            {
                int key = keyIterator.next();
                properties.put( key, cowPropertyAddMap.get( key ) );
            }
        }
        if ( removed != null )
        {
            while ( removed.hasNext() )
            {
                properties.remove( removed.next() );
            }
        }
    }
}
