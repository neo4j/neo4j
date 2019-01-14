/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.util.UUID;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.assertion.Assert;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forRelType;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyConstraintValidationIT.NodeKeyConstraintValidationIT;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyConstraintValidationIT
        .NodePropertyExistenceConstraintValidationIT;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyConstraintValidationIT
        .RelationshipPropertyExistenceConstraintValidationIT;

@RunWith( Suite.class )
@SuiteClasses( {
        NodePropertyExistenceConstraintValidationIT.class,
        RelationshipPropertyExistenceConstraintValidationIT.class,
        NodeKeyConstraintValidationIT.class
} )
public class PropertyConstraintValidationIT
{
    public static class NodeKeyConstraintValidationIT extends NodePropertyExistenceConstraintValidationIT
    {
        @Override
        void createConstraint( String key, String property ) throws KernelException
        {
            TokenWrite tokenWrite = tokenWriteInNewTransaction();
            int label = tokenWrite.labelGetOrCreateForName( key );
            int propertyKey = tokenWrite.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWrite schemaWrite = schemaWriteInNewTransaction();
            schemaWrite.nodeKeyConstraintCreate( forLabel( label, propertyKey ) );
            commit();
        }

        @Test
        public void requirePropertyFromMultipleNodeKeys()
        {
            Label label = Label.label( "multiNodeKeyLabel" );
            SchemaHelper.createNodeKeyConstraint( db, label,  "property1", "property2" );
            SchemaHelper.createNodeKeyConstraint( db, label,  "property2", "property3" );
            SchemaHelper.createNodeKeyConstraint( db, label,  "property3", "property4" );

            Assert.assertException( () ->
            {
                try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
                {
                    Node node = db.createNode( label );
                    node.setProperty( "property1", "1" );
                    node.setProperty( "property2", "2" );
                    transaction.success();
                }
            }, ConstraintViolationException.class,
                    "Node(0) with label `multiNodeKeyLabel` must have the properties `property2, property3`" );

            Assert.assertException( () ->
            {
                try ( org.neo4j.graphdb.Transaction transaction = db.beginTx() )
                {
                    Node node = db.createNode( label );
                    node.setProperty( "property1", "1" );
                    node.setProperty( "property2", "2" );
                    node.setProperty( "property3", "3" );
                    transaction.success();
                }
            }, ConstraintViolationException.class,
                    "Node(1) with label `multiNodeKeyLabel` must have the properties `property3, property4`" );
        }
    }

    public static class NodePropertyExistenceConstraintValidationIT
            extends AbstractPropertyExistenceConstraintValidationIT
    {
        @Test
        public void shouldAllowNoopLabelUpdate() throws Exception
        {
            // given
            long entityId = createConstraintAndEntity( "Label1", "key1", "value1" );

            Transaction transaction = newTransaction( AnonymousContext.writeToken() );

            // when
            int label = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
            transaction.dataWrite().nodeAddLabel( entityId, label );

            // then should not throw exception
        }

        @Override
        void createConstraint( String key, String property ) throws KernelException
        {
            TokenWrite tokenWrite = tokenWriteInNewTransaction();
            int label = tokenWrite.labelGetOrCreateForName( key );
            int propertyKey = tokenWrite.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWrite schemaWrite = schemaWriteInNewTransaction();
            schemaWrite.nodePropertyExistenceConstraintCreate( forLabel( label, propertyKey ) );
            commit();
        }

        @Override
        long createEntity( Transaction transaction, String type ) throws Exception
        {
            long node = transaction.dataWrite().nodeCreate();
            int labelId = transaction.tokenWrite().labelGetOrCreateForName( type );
            transaction.dataWrite().nodeAddLabel( node, labelId );
            return node;
        }

        @Override
        long createEntity( Transaction transaction, String property, String value ) throws Exception
        {
            long node = transaction.dataWrite().nodeCreate();
            int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
            transaction.dataWrite().nodeSetProperty( node, propertyKey, Values.of( value ) );
            return node;
        }

        @Override
        long createEntity( Transaction transaction, String type, String property, String value ) throws Exception
        {
            long node = createEntity( transaction, type );
            int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
            transaction.dataWrite().nodeSetProperty( node, propertyKey, Values.of( value ) );
            return node;
        }

        @Override
        long createConstraintAndEntity( String type, String property, String value ) throws Exception
        {
            Transaction transaction = newTransaction( AnonymousContext.writeToken() );
            int label = transaction.tokenWrite().labelGetOrCreateForName( type );
            long node = transaction.dataWrite().nodeCreate();
            transaction.dataWrite().nodeAddLabel( node, label );
            int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
            transaction.dataWrite().nodeSetProperty( node, propertyKey, Values.of( value ) );
            commit();

            createConstraint( type, property );

            return node;
        }

        @Override
        void setProperty( Write writeOps, long entityId, int propertyKeyId, Value value ) throws Exception
        {
            writeOps.nodeSetProperty( entityId, propertyKeyId, value );
        }

        @Override
        void removeProperty( Write writeOps, long entityId, int propertyKey ) throws Exception
        {
            writeOps.nodeRemoveProperty( entityId, propertyKey );
        }

        @Override
        int entityCount() throws TransactionFailureException
        {
           Transaction transaction = newTransaction();
            int result = countNodes( transaction );
            rollback();
            return result;
        }
    }

    public static class RelationshipPropertyExistenceConstraintValidationIT
            extends AbstractPropertyExistenceConstraintValidationIT
    {
        @Override
        void createConstraint( String key, String property ) throws KernelException
        {
            TokenWrite tokenWrite = tokenWriteInNewTransaction();
            int relTypeId = tokenWrite.relationshipTypeGetOrCreateForName( key );
            int propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWrite schemaWrite = schemaWriteInNewTransaction();
            schemaWrite.relationshipPropertyExistenceConstraintCreate( forRelType( relTypeId, propertyKeyId ) );
            commit();
        }

        @Override
        long createEntity( Transaction transaction, String type ) throws Exception
        {
            long start = transaction.dataWrite().nodeCreate();
            long end = transaction.dataWrite().nodeCreate();
            int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( type );
            return transaction.dataWrite().relationshipCreate(  start, relType, end );
        }

        @Override
        long createEntity( Transaction transaction, String property, String value ) throws Exception
        {
            long start = transaction.dataWrite().nodeCreate();
            long end = transaction.dataWrite().nodeCreate();
            String relationshipTypeName = UUID.randomUUID().toString();
            int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( relationshipTypeName );
            long relationship = transaction.dataWrite().relationshipCreate( start, relType, end );

            int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
            transaction.dataWrite().relationshipSetProperty( relationship, propertyKey, Values.of( value ) );
            return relationship;
        }

        @Override
        long createEntity( Transaction transaction, String type, String property, String value ) throws Exception
        {
            long relationship = createEntity( transaction, type );
            int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
            transaction.dataWrite().relationshipSetProperty( relationship, propertyKey, Values.of( value ) );
            return relationship;
        }

        @Override
        long createConstraintAndEntity( String type, String property, String value ) throws Exception
        {
            Transaction transaction = newTransaction( AnonymousContext.writeToken() );
            int relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName( type );
            long start = transaction.dataWrite().nodeCreate();
            long end = transaction.dataWrite().nodeCreate();
            long relationship = transaction.dataWrite().relationshipCreate( start, relType, end );
            int propertyKey = transaction.tokenWrite().propertyKeyGetOrCreateForName( property );
            transaction.dataWrite().relationshipSetProperty( relationship, propertyKey, Values.of( value ) );
            commit();

            createConstraint( type, property );

            return relationship;
        }

        @Override
        void setProperty( Write writeOps, long entityId, int propertyKeyId, Value value ) throws Exception
        {
            writeOps.relationshipSetProperty( entityId, propertyKeyId, value );
        }

        @Override
        void removeProperty( Write writeOps, long entityId, int propertyKey ) throws Exception
        {
            writeOps.relationshipRemoveProperty( entityId, propertyKey );
        }

        @Override
        int entityCount() throws TransactionFailureException
        {
            Transaction transaction = newTransaction();
            int result = countRelationships( transaction );
            rollback();
            return result;
        }
    }

    public abstract static class AbstractPropertyExistenceConstraintValidationIT extends KernelIntegrationTest
    {
        abstract void createConstraint( String key, String property ) throws KernelException;

        abstract long createEntity( Transaction transaction, String type ) throws Exception;

        abstract long createEntity( Transaction transaction, String property, String value ) throws Exception;

        abstract long createEntity( Transaction transaction, String type, String property, String value )
                throws Exception;

        abstract long createConstraintAndEntity( String type, String property, String value ) throws Exception;

        abstract void setProperty( Write writeOps, long entityId, int propertyKeyId, Value value )
                throws Exception;

        abstract void removeProperty( Write writeOps, long entityId, int propertyKey ) throws Exception;

        abstract int entityCount() throws TransactionFailureException;

        @Override
        protected GraphDatabaseService createGraphDatabase()
        {
            return new TestEnterpriseGraphDatabaseFactory().setFileSystem( fileSystemRule.get() )
                    .newEmbeddedDatabaseBuilder( testDir.graphDbDir() )
                    .newGraphDatabase();
        }

        @Test
        public void shouldEnforcePropertyExistenceConstraintWhenCreatingEntityWithoutProperty() throws Exception
        {
            // given
            createConstraint( "Type1", "key1" );

            Transaction transaction = newTransaction( AnonymousContext.writeToken() );

            // when
            createEntity( transaction, "Type1" );
            try
            {
                commit();
                fail( "should have thrown exception" );
            }
            // then
            catch ( ConstraintViolationTransactionFailureException e )
            {
                Status expected = Status.Schema.ConstraintValidationFailed;
                assertThat( e.status(), is( expected ) );
            }
        }

        @Test
        public void shouldEnforceConstraintWhenRemoving() throws Exception
        {
            // given
            long entity = createConstraintAndEntity( "Type1", "key1", "value1" );
            Transaction transaction = newTransaction( AnonymousContext.writeToken() );

            // when
            int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
            removeProperty( transaction.dataWrite(), entity, key );
            try
            {
                commit();
                fail( "should have thrown exception" );
            }
            // then
            catch ( ConstraintViolationTransactionFailureException e )
            {
                Status expected = Status.Schema.ConstraintValidationFailed;
                assertThat( e.status(), is( expected ) );
            }
        }

        @Test
        public void shouldAllowTemporaryViolationsWithinTransactions() throws Exception
        {
            // given
            long entity = createConstraintAndEntity( "Type1", "key1", "value1" );
            Transaction transaction = newTransaction( AnonymousContext.writeToken() );

            // when
            int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
            //remove and put back
            removeProperty( transaction.dataWrite(), entity, key );
            setProperty( transaction.dataWrite(), entity, key, Values.of( "value2" ) );

            commit();
        }

        @Test
        public void shouldAllowNoopPropertyUpdate() throws Exception
        {
            // given
            long entity = createConstraintAndEntity( "Type1", "key1", "value1" );

            Transaction transaction = newTransaction( AnonymousContext.writeToken() );

            // when
            int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
            setProperty( transaction.dataWrite(), entity, key, Values.of( "value1" ) );

            // then should not throw exception
        }

        @Test
        public void shouldAllowCreationOfNonConflictingData() throws Exception
        {
            // given
            createConstraintAndEntity( "Type1", "key1", "value1" );

            Transaction transaction = newTransaction( AnonymousContext.writeToken() );

            // when
            createEntity( transaction, "key1", "value1" );
            createEntity( transaction, "Type2" );
            createEntity( transaction, "Type1", "key1", "value2" );
            createEntity( transaction, "Type1", "key1", "value3" );

            commit();

            // then
            assertEquals( 5, entityCount() );
        }
    }
}
