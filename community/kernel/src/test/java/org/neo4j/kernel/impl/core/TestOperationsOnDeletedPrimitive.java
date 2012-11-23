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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.impl.core.WritableTransactionState.CowEntityElement;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;

/**
 * To cover cases where either the primitive that a property belongs to has
 * been removed, as well as cases where we are reading a property chain and
 * someone suddenly removes items in the chain.
 *
 * The second case, with inconsistent chains, should not be handled like this later on,
 * we don't want to throw NotFoundException to the user. Rather, we want to introduce appropriate
 * read locks such that we never read a property chain in an inconsistent state.
 *
 * The same thing applies to reading relationship chains.
 *
 * Once we modify HA to take appropriate locks while applying transactions to slaves,
 * this should be ready to be implemented.
 */
public class TestOperationsOnDeletedPrimitive
{
    private NodeManager nodeManager = mockTheNodeManager();
    private PropertyContainer propertyContainer = mock( PropertyContainer.class );
    Primitive primitive = new PrimitiveThatHasActuallyBeenDeleted( false );

    private NodeManager mockTheNodeManager()
    {
        NodeManager mock = mock( NodeManager.class );
        when( mock.getTransactionState() ).thenReturn( TransactionState.NO_STATE );
        return mock;
    }
    
    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundOnGetPropertyWithDefaultOnDeletedEntity() throws Exception
    {
        primitive.getProperty( nodeManager, "the_property", new Object() );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnGetPropertyOnDeletedEntity() throws Exception
    {
        primitive.getProperty( nodeManager, "the_property" );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnSetPropertyOnDeletedEntity() throws Exception
    {
        primitive.setProperty( nodeManager, propertyContainer, "the_property", "the value" );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnRemovePropertyOnDeletedEntity() throws Exception
    {
        primitive.removeProperty( nodeManager, propertyContainer, "the_property" );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnGetPropertyKeysOnDeletedEntity() throws Exception
    {
        primitive.getPropertyKeys( nodeManager );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnGetPropertyValuesOnDeletedEntity() throws Exception
    {
        primitive.getPropertyValues( nodeManager );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnHasPropertyOnDeletedEntity() throws Exception
    {
        primitive.hasProperty( nodeManager, "the_property" );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnGetAllCommittedPropertiesOnDeletedEntity() throws Exception
    {
        primitive.getAllCommittedProperties( nodeManager, nodeManager.getTransactionState() );
    }

    @Test(expected = NotFoundException.class)
    public void shouldThrowNotFoundExceptionOnGetCommittedPropertyValueOnDeletedEntity() throws Exception
    {
        primitive.getCommittedPropertyValue( nodeManager, "the_key", nodeManager.getTransactionState() );
    }

    // Test utils


    private class PrimitiveThatHasActuallyBeenDeleted extends Primitive
    {

        PrimitiveThatHasActuallyBeenDeleted( boolean newPrimitive )
        {
            super( newPrimitive );
        }

        // Because we have been deleted, this always throws invalidRecordException
        @Override
        protected ArrayMap<Integer, PropertyData> loadProperties( NodeManager nodeManager, boolean light )
        {
            throw new InvalidRecordException( "I have been deleted, remember!" );
        }

        @Override
        protected PropertyData changeProperty( NodeManager nodeManager, PropertyData property, Object value, TransactionState tx )
        {
            return null;
        }

        @Override
        protected PropertyData addProperty( NodeManager nodeManager, PropertyIndex index, Object value )
        {
            return null;
        }

        @Override
        protected void removeProperty( NodeManager nodeManager, PropertyData property, TransactionState tx )
        {
        }

        @Override
        public long getId()
        {
            return 0;
        }

        @Override
        protected void setEmptyProperties()
        {
        }

        @Override
        protected PropertyData[] allProperties()
        {
            return null;
        }

        @Override
        protected PropertyData getPropertyForIndex( int keyId )
        {
            return null;
        }

        @Override
        protected void setProperties( ArrayMap<Integer, PropertyData> properties, NodeManager nodeManager )
        {
        }

        @Override
        protected void commitPropertyMaps( ArrayMap<Integer, PropertyData> cowPropertyAddMap, ArrayMap<Integer,
                PropertyData> cowPropertyRemoveMap, long firstProp, NodeManager nodeManager )
        {
        }

        @Override
        public CowEntityElement getEntityElement( PrimitiveElement element, boolean create )
        {
            return null;
        }

        @Override
        PropertyContainer asProxy( NodeManager nm )
        {
            PropertyContainer mockContainer = mock(PropertyContainer.class);
            when( mockContainer.toString() ).thenReturn( "MockedEntity[1337]" );
            return mockContainer;
        }
    }

}
