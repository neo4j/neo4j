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

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.Property;

/**
 * This is an intermediary implementation to allow the store layer to return only the full set of properties.
 * When we have a nice cache layer that caches those property arrays we should implement these methods there, and
 * remove this class.
 */
public class PropertyOperationTranslation extends CompositeStatementContext
{
    public static class TransactionContext extends DelegatingTransactionContext
    {
        public TransactionContext( org.neo4j.kernel.api.TransactionContext delegate )
        {
            super( delegate );
        }

        @Override
        public StatementContext newStatementContext()
        {
            return new PropertyOperationTranslation( super.newStatementContext() );
        }
    }

    public PropertyOperationTranslation( StatementContext delegate )
    {
        super( delegate );
    }

    @Override
    public boolean nodeHasProperty( long nodeId, long propertyKeyId )  throws EntityNotFoundException
    {
        return hasProperty( nodeGetAllProperties( nodeId ), propertyKeyId );
    }

    @Override
    public boolean relationshipHasProperty( long relationshipId, long propertyKeyId ) throws EntityNotFoundException
    {
        return hasProperty( relationshipGetAllProperties( relationshipId ), propertyKeyId );
    }

    private boolean hasProperty( Iterator<Property> properties, long propertyKeyId )
    {
        while (  properties.hasNext() )
        {
            Property property = properties.next();
            if ( property.propertyKeyId() == propertyKeyId )
            {
                return true;
            }
        }
        return false;
    }
}
