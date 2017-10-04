/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

public class GraphPropertiesProxy implements GraphProperties
{
    private final GraphPropertiesActions actions;

    public interface GraphPropertiesActions
    {
        Statement statement();

        GraphDatabaseService getGraphDatabaseService();

        void failTransaction();
    }

    public GraphPropertiesProxy( GraphPropertiesActions actions )
    {
        this.actions = actions;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return actions.getGraphDatabaseService();
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
            return statement.readOperations().graphHasProperty( propertyKeyId );
        }
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = actions.statement() )
        {
            try
            {
                int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
                if ( propertyKeyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
                {
                    throw new NotFoundException( format( "No such property, '%s'.", key ) );
                }

                Value value = statement.readOperations().graphGetProperty( propertyKeyId );

                if ( value == Values.NO_VALUE )
                {
                    throw new PropertyNotFoundException( propertyKeyId, EntityType.GRAPH, -1 );
                }

                return value.asObjectCopy();
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

        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
            Value value = statement.readOperations().graphGetProperty( propertyKeyId );
            return value == Values.NO_VALUE ? defaultValue : value.asObjectCopy();
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            try
            {
                statement.dataWriteOperations().graphSetProperty( propertyKeyId, Values.of( value, false ) );
            }
            catch ( IllegalArgumentException e )
            {
                // Trying to set an illegal value is a critical error - fail this transaction
                actions.failTransaction();
                throw e;
            }
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            return statement.dataWriteOperations().graphRemoveProperty( propertyKeyId ).asObjectCopy();
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( Statement statement = actions.statement() )
        {
            List<String> keys = new ArrayList<>();
            PrimitiveIntIterator properties = statement.readOperations().graphGetPropertyKeys();
            while ( properties.hasNext() )
            {
                keys.add( statement.readOperations().propertyKeyGetName( properties.next() ) );
            }
            return keys;
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
    }

    @Override
    public Map<String, Object> getProperties( String... names )
    {
        try ( Statement statement = actions.statement() )
        {
            Map<String, Object> properties = new HashMap<>();
            ReadOperations readOperations = statement.readOperations();
            for ( String name : names )
            {
                int propertyKeyId = readOperations.propertyKeyGetForName( name );
                Object value = readOperations.graphGetProperty( propertyKeyId );
                if ( value != null )
                {
                    properties.put( name, value );
                }
            }
            return properties;
        }
    }

    @Override
    public Map<String, Object> getAllProperties()
    {
        try ( Statement statement = actions.statement() )
        {
            Map<String, Object> properties = new HashMap<>();
            ReadOperations readOperations = statement.readOperations();
            PrimitiveIntIterator propertyKeys = readOperations.graphGetPropertyKeys();
            while ( propertyKeys.hasNext() )
            {
                int propertyKeyId = propertyKeys.next();
                properties.put( readOperations.propertyKeyGetName( propertyKeyId ),
                        readOperations.graphGetProperty( propertyKeyId ).asObjectCopy() );
            }
            return properties;
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        // Yeah, this is breaking transitive equals, but should be OK anyway.
        // Also, we're checking == (not .equals) on GDS since that seems to be what the tests are asserting
        return o instanceof GraphPropertiesProxy &&
                actions.getGraphDatabaseService() == ((GraphPropertiesProxy)o).actions.getGraphDatabaseService();
    }

    @Override
    public int hashCode()
    {
        return actions.getGraphDatabaseService().hashCode();
    }
}
