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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.properties.Property;

import static org.neo4j.helpers.collection.Iterables.map;

/**
 * This is an intermediary implementation to allow the store layer to return only the full set of properties.
 * When we have a nice cache layer that caches those property arrays we should implement these methods there, and
 * remove this class.
 */
public class StoreOperationTranslatingStatementContext extends CompositeStatementContext
{
    public StoreOperationTranslatingStatementContext()
    {
    }

    public StoreOperationTranslatingStatementContext( StatementContext delegate, SchemaStateOperations
            schemaStateOperations )
    {
        super( delegate, schemaStateOperations );
    }

    @Override
    public Iterator<Long> nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException
    {
        return map( new Function<Property, Long>()
        {
            @Override
            public Long apply( Property property )
            {
                return property.propertyKeyId();
            }
        }, nodeGetAllProperties( nodeId ) );
    }

    @Override
    public Iterator<Long> relationshipGetPropertyKeys( long relationshipId ) throws EntityNotFoundException
    {
        return map( new Function<Property, Long>()
        {
            @Override
            public Long apply( Property property )
            {
                return property.propertyKeyId();
            }
        }, relationshipGetAllProperties( relationshipId ) );
    }

    @Override
    public boolean nodeHasProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException

    {
        return null != getPropertyOrNull( nodeGetAllProperties( nodeId ), propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException

    {
        return null != getPropertyOrNull( relationshipGetAllProperties( relationshipId ), propertyKeyId );
    }

    @Override
    public Property nodeGetProperty( long nodeId, long propertyKeyId ) throws EntityNotFoundException
    {
        Property property = getPropertyOrNull( nodeGetAllProperties( nodeId ), propertyKeyId );
        return property == null ? Property.noNodeProperty( nodeId, propertyKeyId ) : property;
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, long propertyKeyId ) throws EntityNotFoundException
    {
        Property property = getPropertyOrNull( relationshipGetAllProperties( relationshipId ), propertyKeyId );
        return property == null ? Property.noRelationshipProperty( relationshipId, propertyKeyId ) : property;
    }

    private Property getPropertyOrNull( Iterator<Property> properties, long propertyKeyId )
    {
        while ( properties.hasNext() )
        {
            Property property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return property;
            }
        }
        return null;
    }
}
