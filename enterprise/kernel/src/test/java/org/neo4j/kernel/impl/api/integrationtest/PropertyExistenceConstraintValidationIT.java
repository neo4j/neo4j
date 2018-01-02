/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.integrationtest;

import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyExistenceConstraintValidationIT.NodePropertyExistenceExistenceConstraintValidationIT;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyExistenceConstraintValidationIT.RelationshipPropertyExistenceExistenceConstraintValidationIT;

@RunWith( Suite.class )
@SuiteClasses( {
        NodePropertyExistenceExistenceConstraintValidationIT.class,
        RelationshipPropertyExistenceExistenceConstraintValidationIT.class
} )
public class PropertyExistenceConstraintValidationIT
{
    public static class NodePropertyExistenceExistenceConstraintValidationIT
            extends AbstractPropertyExistenceConstraintValidationIT
    {
        @Test
        public void shouldAllowNoopLabelUpdate() throws Exception
        {
            // given
            long entityId = createConstraintAndEntity( "Label1", "key1", "value1" );

            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // when
            statement.nodeAddLabel( entityId, statement.labelGetOrCreateForName( "Label1" ) );

            // then should not throw exception
        }

        @Override
        void createConstraint( String key, String property ) throws KernelException
        {
            DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
            int label = dataWrite.labelGetOrCreateForName( key );
            int propertyKey = dataWrite.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWriteOperations schemaWrite = schemaWriteOperationsInNewTransaction();
            schemaWrite.nodePropertyExistenceConstraintCreate( label, propertyKey );
            commit();
        }

        @Override
        long createEntity( DataWriteOperations writeOps, String type ) throws Exception
        {
            long node = writeOps.nodeCreate();
            writeOps.nodeAddLabel( node, writeOps.labelGetOrCreateForName( type ) );
            return node;
        }

        @Override
        long createEntity( DataWriteOperations writeOps, String property, String value ) throws Exception
        {
            long node = writeOps.nodeCreate();
            int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
            writeOps.nodeSetProperty( node, Property.property( propertyKey, value ) );
            return node;
        }

        @Override
        long createEntity( DataWriteOperations writeOps, String type, String property, String value ) throws Exception
        {
            long node = createEntity( writeOps, type );
            int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
            writeOps.nodeSetProperty( node, Property.property( propertyKey, value ) );
            return node;
        }

        @Override
        long createConstraintAndEntity( String type, String property, String value ) throws Exception
        {
            DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
            int label = dataWrite.labelGetOrCreateForName( type );
            long node = dataWrite.nodeCreate();
            dataWrite.nodeAddLabel( node, label );
            int propertyKey = dataWrite.propertyKeyGetOrCreateForName( property );
            dataWrite.nodeSetProperty( node, Property.property( propertyKey, value ) );
            commit();

            createConstraint( type, property );

            return node;
        }

        @Override
        void setProperty( DataWriteOperations writeOps, long entityId, DefinedProperty property ) throws Exception
        {
            writeOps.nodeSetProperty( entityId, property );
        }

        @Override
        void removeProperty( DataWriteOperations writeOps, long entityId, int propertyKey ) throws Exception
        {
            writeOps.nodeRemoveProperty( entityId, propertyKey );
        }

        @Override
        int entityCount() throws TransactionFailureException
        {
            ReadOperations readOps = readOperationsInNewTransaction();
            int result = PrimitiveLongCollections.count( readOps.nodesGetAll() );
            rollback();
            return result;
        }
    }

    public static class RelationshipPropertyExistenceExistenceConstraintValidationIT
            extends AbstractPropertyExistenceConstraintValidationIT
    {
        @Override
        void createConstraint( String key, String property ) throws KernelException
        {
            DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
            int relTypeId = dataWrite.relationshipTypeGetOrCreateForName( key );
            int propertyKeyId = dataWrite.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWriteOperations schemaWrite = schemaWriteOperationsInNewTransaction();
            schemaWrite.relationshipPropertyExistenceConstraintCreate( relTypeId, propertyKeyId );
            commit();
        }

        @Override
        long createEntity( DataWriteOperations writeOps, String type ) throws Exception
        {
            long start = writeOps.nodeCreate();
            long end = writeOps.nodeCreate();
            int relType = writeOps.relationshipTypeGetOrCreateForName( type );
            return writeOps.relationshipCreate( relType, start, end );
        }

        @Override
        long createEntity( DataWriteOperations writeOps, String property, String value ) throws Exception
        {
            long start = writeOps.nodeCreate();
            long end = writeOps.nodeCreate();
            int relType = writeOps.relationshipTypeGetOrCreateForName( UUID.randomUUID().toString() );
            long relationship = writeOps.relationshipCreate( relType, start, end );

            int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
            writeOps.relationshipSetProperty( relationship, Property.property( propertyKey, value ) );
            return relationship;
        }

        @Override
        long createEntity( DataWriteOperations writeOps, String type, String property, String value ) throws Exception
        {
            long relationship = createEntity( writeOps, type );
            int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
            writeOps.relationshipSetProperty( relationship, Property.property( propertyKey, value ) );
            return relationship;
        }

        @Override
        long createConstraintAndEntity( String type, String property, String value ) throws Exception
        {
            DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
            int relType = dataWrite.relationshipTypeGetOrCreateForName( type );
            long start = dataWrite.nodeCreate();
            long end = dataWrite.nodeCreate();
            long relationship = dataWrite.relationshipCreate( relType, start, end );
            int propertyKey = dataWrite.propertyKeyGetOrCreateForName( property );
            dataWrite.relationshipSetProperty( relationship, Property.property( propertyKey, value ) );
            commit();

            createConstraint( type, property );

            return relationship;
        }

        @Override
        void setProperty( DataWriteOperations writeOps, long entityId, DefinedProperty property ) throws Exception
        {
            writeOps.relationshipSetProperty( entityId, property );
        }

        @Override
        void removeProperty( DataWriteOperations writeOps, long entityId, int propertyKey ) throws Exception
        {
            writeOps.relationshipRemoveProperty( entityId, propertyKey );
        }

        @Override
        int entityCount() throws TransactionFailureException
        {
            ReadOperations readOps = readOperationsInNewTransaction();
            int result = PrimitiveLongCollections.count( readOps.relationshipsGetAll() );
            rollback();
            return result;
        }
    }

    public abstract static class AbstractPropertyExistenceConstraintValidationIT extends KernelIntegrationTest
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

        @Override
        protected GraphDatabaseService createGraphDatabase( EphemeralFileSystemAbstraction fs )
        {
            return new TestEnterpriseGraphDatabaseFactory()
                    .setFileSystem( fs )
                    .newImpermanentDatabaseBuilder()
                    .newGraphDatabase();
        }

        @Test
        public void shouldEnforcePropertyExistenceConstraintWhenCreatingEntityWithoutProperty() throws Exception
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
}
