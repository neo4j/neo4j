/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

public class GraphPropertiesProxy implements GraphProperties
{
    private final EmbeddedProxySPI actions;

    public GraphPropertiesProxy( EmbeddedProxySPI actions )
    {
        this.actions = actions;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return actions.getGraphDatabase();
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        KernelTransaction transaction = safeAcquireTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return false;
        }

        PropertyCursor properties = transaction.ambientPropertyCursor();
        transaction.dataRead().graphProperties( properties );
        while ( properties.next() )
        {
            if ( propertyKey == properties.propertyKey() )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }
        KernelTransaction transaction = safeAcquireTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            throw new NotFoundException( format( "No such property, '%s'.", key ) );
        }

        PropertyCursor properties = transaction.ambientPropertyCursor();
        transaction.dataRead().graphProperties( properties );

        while ( properties.next() )
        {
            if ( propertyKey == properties.propertyKey() )
            {
                Value value = properties.propertyValue();
                if ( value == Values.NO_VALUE )
                {
                    throw new NotFoundException( format( "No such property, '%s'.", key ) );
                }
                return value.asObjectCopy();
            }
        }
        throw new NotFoundException( format( "No such property, '%s'.", key ) );
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }
        KernelTransaction transaction = safeAcquireTransaction();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return defaultValue;
        }
        transaction.dataRead().graphProperties( properties );
        while ( properties.next() )
        {
            if ( propertyKey == properties.propertyKey() )
            {
                Value value = properties.propertyValue();
                return value == Values.NO_VALUE ? defaultValue : value.asObjectCopy();
            }
        }
        return defaultValue;
    }

    @Override
    public void setProperty( String key, Object value )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        int propertyKeyId;
        try
        {
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }

        try ( Statement ignore = transaction.acquireStatement() )
        {
            transaction.dataWrite().graphSetProperty( propertyKeyId, Values.of( value, false ) );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        int propertyKeyId;
        try
        {
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        try ( Statement ignore = transaction.acquireStatement() )
        {
            return transaction.dataWrite().graphRemoveProperty( propertyKeyId ).asObjectCopy();
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        KernelTransaction transaction = safeAcquireTransaction();
        List<String> keys = new ArrayList<>();
        try
        {
            PropertyCursor properties = transaction.ambientPropertyCursor();
            TokenRead token = transaction.tokenRead();
            transaction.dataRead().graphProperties( properties );
            while ( properties.next() )
            {
                keys.add( token.propertyKeyName( properties.propertyKey() ) );
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
        return keys;
    }

    @Override
    public Map<String,Object> getProperties( String... names )
    {
        Objects.requireNonNull( names, "Properties keys should be not null array." );

        if ( names.length == 0 )
        {
            return Collections.emptyMap();
        }

        KernelTransaction transaction = safeAcquireTransaction();

        int itemsToReturn = names.length;
        Map<String,Object> properties = new HashMap<>( itemsToReturn );
        TokenRead token = transaction.tokenRead();

        //Find ids, note we are betting on that the number of keys
        //is small enough not to use a set here.
        int[] propertyIds = new int[itemsToReturn];
        for ( int i = 0; i < itemsToReturn; i++ )
        {
            String key = names[i];
            if ( key == null )
            {
                throw new NullPointerException( String.format( "Key %d was null", i ) );
            }
            propertyIds[i] = token.propertyKey( key );
        }

        PropertyCursor propertyCursor = transaction.ambientPropertyCursor();
        transaction.dataRead().graphProperties( propertyCursor );
        int propertiesToFind = itemsToReturn;
        while ( propertiesToFind > 0 && propertyCursor.next() )
        {
            //Do a linear check if this is a property we are interested in.
            int currentKey = propertyCursor.propertyKey();
            for ( int i = 0; i < itemsToReturn; i++ )
            {
                if ( propertyIds[i] == currentKey )
                {
                    properties.put( names[i],
                            propertyCursor.propertyValue().asObjectCopy() );
                    propertiesToFind--;
                    break;
                }
            }
        }
        return properties;
    }

    @Override
    public Map<String, Object> getAllProperties()
    {
        KernelTransaction transaction = safeAcquireTransaction();
        Map<String,Object> properties = new HashMap<>();

        try
        {
            PropertyCursor propertyCursor = transaction.ambientPropertyCursor();
            TokenRead token = transaction.tokenRead();
            transaction.dataRead().graphProperties( propertyCursor );
            while ( propertyCursor.next() )
            {
                properties.put( token.propertyKeyName( propertyCursor.propertyKey() ),
                        propertyCursor.propertyValue().asObjectCopy() );
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
        return properties;
    }

    @Override
    public boolean equals( Object o )
    {
        // Yeah, this is breaking transitive equals, but should be OK anyway.
        // Also, we're checking == (not .equals) on GDS since that seems to be what the tests are asserting
        return o instanceof GraphPropertiesProxy &&
                actions.getGraphDatabase() == ((GraphPropertiesProxy)o).actions.getGraphDatabase();
    }

    @Override
    public int hashCode()
    {
        return actions.getGraphDatabase().hashCode();
    }

    private KernelTransaction safeAcquireTransaction()
    {
        org.neo4j.kernel.api.KernelTransaction transaction = actions.kernelTransaction();
        if ( transaction.isTerminated() )
        {
            Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
            throw new TransactionTerminatedException( terminationReason );
        }
        return transaction;
    }
}
