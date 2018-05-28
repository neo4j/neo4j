/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.assertion.Assert;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.properties.Property.property;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forRelType;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyConstraintValidationIT.NodeKeyConstraintValidationIT;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyConstraintValidationIT.NodePropertyExistenceConstraintValidationIT;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyConstraintValidationIT.RelationshipPropertyExistenceConstraintValidationIT;

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
            TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
            int label = tokenWriteOperations.labelGetOrCreateForName( key );
            int propertyKey = tokenWriteOperations.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWriteOperations schemaWrite = schemaWriteOperationsInNewTransaction();
            schemaWrite.nodeKeyConstraintCreate( forLabel( label, propertyKey ) );
            commit();
        }

        @Test
        public void requirePropertyFromMultipleNodeKeys() throws Exception
        {
            Label label = Label.label( "multiNodeKeyLabel" );
            SchemaHelper.createNodeKeyConstraint( db, label,  "property1", "property2" );
            SchemaHelper.createNodeKeyConstraint( db, label,  "property2", "property3" );
            SchemaHelper.createNodeKeyConstraint( db, label,  "property3", "property4" );

            Assert.assertException( () ->
            {
                try ( Transaction transaction = db.beginTx() )
                {
                    Node node = db.createNode( label );
                    node.setProperty( "property1", "1" );
                    node.setProperty( "property2", "2" );
                    transaction.success();
                }
            }, ConstraintViolationException.class, "Node(0) with label `multiNodeKeyLabel` must have the properties `property2, property3`" );

            Assert.assertException( () ->
            {
                try ( Transaction transaction = db.beginTx() )
                {
                    Node node = db.createNode( label );
                    node.setProperty( "property1", "1" );
                    node.setProperty( "property2", "2" );
                    node.setProperty( "property3", "3" );
                    transaction.success();
                }
            }, ConstraintViolationException.class, "Node(1) with label `multiNodeKeyLabel` must have the properties `property3, property4`" );
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

            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // when
            int label = statement.tokenWriteOperations().labelGetOrCreateForName( "Label1" );
            statement.dataWriteOperations().nodeAddLabel( entityId, label );

            // then should not throw exception
        }

        @Override
        void createConstraint( String key, String property ) throws KernelException
        {
            TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
            int label = tokenWriteOperations.labelGetOrCreateForName( key );
            int propertyKey = tokenWriteOperations.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWriteOperations schemaWrite = schemaWriteOperationsInNewTransaction();
            schemaWrite.nodePropertyExistenceConstraintCreate( forLabel( label, propertyKey ) );
            commit();
        }

        @Override
        long createEntity( Statement statement, String type ) throws Exception
        {
            long node = statement.dataWriteOperations().nodeCreate();
            int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( type );
            statement.dataWriteOperations().nodeAddLabel( node, labelId );
            return node;
        }

        @Override
        long createEntity( Statement statement, String property, String value ) throws Exception
        {
            long node = statement.dataWriteOperations().nodeCreate();
            int propertyKey = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( property );
            statement.dataWriteOperations().nodeSetProperty( node, property( propertyKey, value ) );
            return node;
        }

        @Override
        long createEntity( Statement statement, String type, String property, String value ) throws Exception
        {
            long node = createEntity( statement, type );
            int propertyKey = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( property );
            statement.dataWriteOperations().nodeSetProperty( node, property( propertyKey, value ) );
            return node;
        }

        @Override
        long createConstraintAndEntity( String type, String property, String value ) throws Exception
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            int label = statement.tokenWriteOperations().labelGetOrCreateForName( type );
            long node = statement.dataWriteOperations().nodeCreate();
            statement.dataWriteOperations().nodeAddLabel( node, label );
            int propertyKey = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( property );
            statement.dataWriteOperations().nodeSetProperty( node, property( propertyKey, value ) );
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

    public static class RelationshipPropertyExistenceConstraintValidationIT
            extends AbstractPropertyExistenceConstraintValidationIT
    {
        @Override
        void createConstraint( String key, String property ) throws KernelException
        {
            TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
            int relTypeId = tokenWriteOperations.relationshipTypeGetOrCreateForName( key );
            int propertyKeyId = tokenWriteOperations.propertyKeyGetOrCreateForName( property );
            commit();

            SchemaWriteOperations schemaWrite = schemaWriteOperationsInNewTransaction();
            schemaWrite.relationshipPropertyExistenceConstraintCreate( forRelType( relTypeId, propertyKeyId ) );
            commit();
        }

        @Override
        long createEntity( Statement statement, String type ) throws Exception
        {
            long start = statement.dataWriteOperations().nodeCreate();
            long end = statement.dataWriteOperations().nodeCreate();
            int relType = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( type );
            return statement.dataWriteOperations().relationshipCreate( relType, start, end );
        }

        @Override
        long createEntity( Statement statement, String property, String value ) throws Exception
        {
            long start = statement.dataWriteOperations().nodeCreate();
            long end = statement.dataWriteOperations().nodeCreate();
            String relationshipTypeName = UUID.randomUUID().toString();
            int relType = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( relationshipTypeName );
            long relationship = statement.dataWriteOperations().relationshipCreate( relType, start, end );

            int propertyKey = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( property );
            statement.dataWriteOperations().relationshipSetProperty( relationship, property( propertyKey, value ) );
            return relationship;
        }

        @Override
        long createEntity( Statement statement, String type, String property, String value ) throws Exception
        {
            long relationship = createEntity( statement, type );
            int propertyKey = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( property );
            statement.dataWriteOperations().relationshipSetProperty( relationship, property( propertyKey, value ) );
            return relationship;
        }

        @Override
        long createConstraintAndEntity( String type, String property, String value ) throws Exception
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            int relType = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( type );
            long start = statement.dataWriteOperations().nodeCreate();
            long end = statement.dataWriteOperations().nodeCreate();
            long relationship = statement.dataWriteOperations().relationshipCreate( relType, start, end );
            int propertyKey = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( property );
            statement.dataWriteOperations().relationshipSetProperty( relationship, property( propertyKey, value ) );
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

        abstract long createEntity( Statement statement, String type ) throws Exception;

        abstract long createEntity( Statement statement, String property, String value ) throws Exception;

        abstract long createEntity( Statement statement, String type, String property, String value )
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

            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

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
                Status expected = Status.Schema.ConstraintValidationFailed;
                assertThat( e.status(), is( expected ) );
            }
        }

        @Test
        public void shouldEnforceConstraintWhenRemoving() throws Exception
        {
            // given
            long entity = createConstraintAndEntity( "Type1", "key1", "value1" );
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // when
            int key = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "key1" );
            removeProperty( statement.dataWriteOperations(), entity, key );
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
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // when
            int key = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "key1" );
            //remove and put back
            removeProperty( statement.dataWriteOperations(), entity, key );
            setProperty( statement.dataWriteOperations(), entity, property( key, "value2" ) );

            commit();
        }

        @Test
        public void shouldAllowNoopPropertyUpdate() throws Exception
        {
            // given
            long entity = createConstraintAndEntity( "Type1", "key1", "value1" );

            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // when
            int key = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "key1" );
            setProperty( statement.dataWriteOperations(), entity, property( key, "value1" ) );

            // then should not throw exception
        }

        @Test
        public void shouldAllowCreationOfNonConflictingData() throws Exception
        {
            // given
            createConstraintAndEntity( "Type1", "key1", "value1" );

            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // when
            createEntity( statement, "key1", "value1" );
            createEntity( statement, "Type2" );
            createEntity( statement, "Type1", "key1", "value2" );
            createEntity( statement, "Type1", "key1", "value3" );

            commit();

            // then
            assertEquals( 5, entityCount() );
        }
    }
}
