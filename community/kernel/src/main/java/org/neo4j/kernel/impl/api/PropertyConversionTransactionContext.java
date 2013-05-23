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
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.Property;

public class PropertyConversionTransactionContext extends DelegatingTransactionContext
{
    public PropertyConversionTransactionContext( TransactionContext delegate )
    {
        super( delegate );
    }

    @Override
    public StatementContext newStatementContext()
    {
        return new PropertyConversionStatementContext( super.newStatementContext() );
    }

    public static class PropertyConversionStatementContext extends CompositeStatementContext
    {
        public PropertyConversionStatementContext( StatementContext delegate )
        {
            super( delegate );
        }

        // nodes

        @Override
        public Object nodeGetPropertyValue( long nodeId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, PropertyNotFoundException, EntityNotFoundException
        {
            return nodeGetProperty( nodeId, propertyKeyId ).value();
        }

        @Override
        public void nodeSetPropertyValue( long nodeId, long propertyKeyId, Object value )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            nodeSetProperty( nodeId, Property.property( propertyKeyId, value ) );
        }

        @Override
        public Property nodeGetProperty( long nodeId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            return super.nodeGetProperty( nodeId, propertyKeyId );
        }

        @Override
        public void nodeSetProperty( long nodeId, Property property )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            super.nodeSetProperty( nodeId, property );
        }

        @Override
        public boolean nodeHasProperty( long nodeId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            return super.nodeHasProperty( nodeId, propertyKeyId );
        }

        @Override
        public Iterator<Long> nodeGetPropertyKeys( long nodeId )
        {
            return super.nodeGetPropertyKeys( nodeId );
        }

        @Override
        public Iterator<Property> nodeGetAllProperties( long nodeId )
        {
            return super.nodeGetAllProperties( nodeId );
        }

        @Override
        public Object nodeRemoveProperty( long nodeId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            return super.nodeRemoveProperty( nodeId, propertyKeyId );
        }

        // relationships

        @Override
        public Object relationshipGetPropertyValue( long relationshipId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, PropertyNotFoundException, EntityNotFoundException
        {
            return relationshipGetProperty( relationshipId, propertyKeyId ).value();
        }

        @Override
        public void relationshipSetPropertyValue( long relationshipId, long propertyKeyId, Object value )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            relationshipSetProperty( relationshipId, Property.property( propertyKeyId, value ) );
        }

        @Override
        public Property relationshipGetProperty( long relationshipId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            return super.relationshipGetProperty( relationshipId, propertyKeyId );
        }

        @Override
        public void relationshipSetProperty( long relationshipId, Property property )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            super.relationshipSetProperty( relationshipId, property );
        }

        @Override
        public boolean relationshipHasProperty( long relationshipId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            return super.relationshipHasProperty( relationshipId, propertyKeyId );
        }

        @Override
        public Iterator<Long> relationshipGetPropertyKeys( long relationshipId )
        {
            return super.relationshipGetPropertyKeys( relationshipId );
        }

        @Override
        public Iterator<Property> relationshipGetAllProperties( long relationshipId )
        {
            return super.relationshipGetAllProperties( relationshipId );
        }

        @Override
        public Object relationshipRemoveProperty( long relationshipId, long propertyKeyId )
                throws PropertyKeyIdNotFoundException, EntityNotFoundException
        {
            return super.relationshipRemoveProperty( relationshipId, propertyKeyId );
        }
    }
}
