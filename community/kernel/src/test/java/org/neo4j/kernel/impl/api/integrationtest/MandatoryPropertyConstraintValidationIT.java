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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class MandatoryPropertyConstraintValidationIT extends KernelIntegrationTest
{
    abstract void createConstraint( String key, String property ) throws KernelException;

    abstract long createEntity( DataWriteOperations writeOps, String type ) throws Exception;

    abstract long createEntity( DataWriteOperations writeOps, String property, String value ) throws Exception;

    abstract long createEntity( DataWriteOperations writeOps, String type, String property, String value )
            throws Exception;

    abstract long createConstraintAndEntity( String type, String property, String value ) throws Exception;

    abstract void setProperty( DataWriteOperations writeOps, long entityId, DefinedProperty property )
            throws Exception;

    abstract void removeProperty( DataWriteOperations writeOps, long entityId, int propertyKey ) throws Exception;

    abstract int entityCount() throws TransactionFailureException;

    @Test
    public void shouldEnforceMandatoryConstraintWhenCreatingEntityWithoutProperty() throws Exception
    {
        // given
        createConstraint( "Type1", "key1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        createEntity( statement, "Type1" );
        try
        {
            commit();
            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationTransactionFailureException e )
        {
            Status expected = Status.Schema.ConstraintViolation;
            assertThat( e.status(), is( expected ) );
        }
    }

    @Test
    public void shouldEnforceConstraintWhenRemoving() throws Exception
    {
        // given
        long entity = createConstraintAndEntity( "Type1", "key1", "value1" );
        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        int key = statement.propertyKeyGetOrCreateForName( "key1" );
        removeProperty( statement, entity, key );
        try
        {
            commit();
            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationTransactionFailureException e )
        {
            Status expected = Status.Schema.ConstraintViolation;
            assertThat( e.status(), is( expected ) );
        }
    }

    @Test
    public void shouldAllowTemporaryViolationsWithinTransactions() throws Exception
    {
        // given
        long entity = createConstraintAndEntity( "Type1", "key1", "value1" );
        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        int key = statement.propertyKeyGetOrCreateForName( "key1" );
        //remove and put back
        removeProperty( statement, entity, key );
        setProperty( statement, entity, Property.property( key, "value2" ) );

        commit();
    }

    @Test
    public void shouldAllowNoopPropertyUpdate() throws Exception
    {
        // given
        long entity = createConstraintAndEntity( "Type1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        int key = statement.propertyKeyGetOrCreateForName( "key1" );
        setProperty( statement, entity, Property.property( key, "value1" ) );

        // then should not throw exception
    }

    @Test
    public void shouldAllowCreationOfNonConflictingData() throws Exception
    {
        // given
        createConstraintAndEntity( "Type1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        createEntity( statement, "key1", "value1" );
        createEntity( statement, "Type2" );
        createEntity( statement, "Type1", "key1", "value2" );
        createEntity( statement, "Type1", "key1", "value1" );

        commit();

        // then
        assertEquals( 5, entityCount() );
    }
}
