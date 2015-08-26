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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.function.Function;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.IndexDescriptor;
import org.neo4j.kernel.properties.Property;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Suite.SuiteClasses;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.impl.api.integrationtest.ConstraintCreationIT.*;

@RunWith( Suite.class )
@SuiteClasses( {
        NodePropertyExistenceConstraintCreationIT.class,
        RelationshipPropertyExistenceConstraintCreationIT.class,
        UniquenessConstraintCreationIT.class
} )
public class ConstraintCreationIT
{
    public static class NodePropertyExistenceConstraintCreationIT
            extends AbstractConstraintCreationIT<NodePropertyExistenceConstraint>
    {
        @Override
        int initializeLabelOrRelType( SchemaWriteOperations writeOps, String name ) throws KernelException
        {
            return writeOps.labelGetOrCreateForName( name );
        }

        @Override
        NodePropertyExistenceConstraint createConstraint( SchemaWriteOperations writeOps, int type, int property )
                throws Exception
        {
            return writeOps.nodePropertyExistenceConstraintCreate( type, property );
        }

        @Override
        void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
        {
            db.schema().constraintFor( label( type ) ).assertPropertyExists( property ).create();
        }

        @Override
        NodePropertyExistenceConstraint newConstraintObject( int type, int property )
        {
            return new NodePropertyExistenceConstraint( type, property );
        }

        @Override
        void dropConstraint( SchemaWriteOperations writeOps, NodePropertyExistenceConstraint constraint )
                throws Exception
        {
            writeOps.constraintDrop( constraint );
        }

        @Override
        void createOffendingDataInRunningTx( GraphDatabaseService db )
        {
            db.createNode( label( KEY ) );
        }

        @Override
        void removeOffendingDataInRunningTx( GraphDatabaseService db )
        {
            try ( ResourceIterator<Node> nodes = db.findNodes( label( KEY ) ) )
            {
                while ( nodes.hasNext() )
                {
                    nodes.next().delete();
                }
            }
        }

        @Test
        public void shouldNotDropPropertyExistenceConstraintThatDoesNotExistWhenThereIsAUniquePropertyConstraint()
                throws Exception
        {
            // given
            UniquenessConstraint constraint;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                constraint = statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
                commit();
            }

            // when
            try
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                statement.constraintDrop(
                        new NodePropertyExistenceConstraint( constraint.label(), constraint.propertyKey() ) );

                fail( "expected exception" );
            }
            // then
            catch ( DropConstraintFailureException e )
            {
                assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
            }
            finally
            {
                rollback();
            }

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();

                Iterator<NodePropertyConstraint> constraints =
                        statement.constraintsGetForLabelAndPropertyKey( typeId, propertyKeyId );

                assertEquals( constraint, single( constraints ) );
            }
        }
    }

    public static class RelationshipPropertyExistenceConstraintCreationIT
            extends AbstractConstraintCreationIT<RelationshipPropertyExistenceConstraint>
    {
        @Override
        int initializeLabelOrRelType( SchemaWriteOperations writeOps, String name ) throws KernelException
        {
            return writeOps.relationshipTypeGetOrCreateForName( name );
        }

        @Override
        RelationshipPropertyExistenceConstraint createConstraint( SchemaWriteOperations writeOps, int type,
                int property ) throws Exception
        {
            return writeOps.relationshipPropertyExistenceConstraintCreate( type, property );
        }

        @Override
        void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
        {
            db.schema().constraintFor( withName( type ) ).assertPropertyExists( property ).create();
        }

        @Override
        RelationshipPropertyExistenceConstraint newConstraintObject( int type, int property )
        {
            return new RelationshipPropertyExistenceConstraint( type, property );
        }

        @Override
        void dropConstraint( SchemaWriteOperations writeOps, RelationshipPropertyExistenceConstraint constraint )
                throws Exception
        {
            writeOps.constraintDrop( constraint );
        }

        @Override
        void createOffendingDataInRunningTx( GraphDatabaseService db )
        {
            Node start = db.createNode();
            Node end = db.createNode();
            start.createRelationshipTo( end, withName( KEY ) );
        }

        @Override
        void removeOffendingDataInRunningTx( GraphDatabaseService db )
        {
            Iterable<Relationship> relationships = GlobalGraphOperations.at( db ).getAllRelationships();
            for ( Relationship relationship : relationships )
            {
                relationship.delete();
            }
        }
    }

    public static class UniquenessConstraintCreationIT extends AbstractConstraintCreationIT<UniquenessConstraint>
    {
        private static final String DUPLICATED_VALUE = "apa";

        @Rule
        public final TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

        @Rule
        public final OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

        @Override
        int initializeLabelOrRelType( SchemaWriteOperations writeOps, String name ) throws KernelException
        {
            return writeOps.labelGetOrCreateForName( KEY );
        }

        @Override
        UniquenessConstraint createConstraint( SchemaWriteOperations writeOps, int type, int property ) throws Exception
        {
            return writeOps.uniquePropertyConstraintCreate( type, property );
        }

        @Override
        void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
        {
            db.schema().constraintFor( label( type ) ).assertPropertyIsUnique( property ).create();
        }

        @Override
        UniquenessConstraint newConstraintObject( int type, int property )
        {
            return new UniquenessConstraint( type, property );
        }

        @Override
        void dropConstraint( SchemaWriteOperations writeOps, UniquenessConstraint constraint ) throws Exception
        {
            writeOps.constraintDrop( constraint );
        }

        @Override
        void createOffendingDataInRunningTx( GraphDatabaseService db )
        {
            db.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
            db.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
        }

        @Override
        void removeOffendingDataInRunningTx( GraphDatabaseService db )
        {
            try ( ResourceIterator<Node> nodes = db.findNodes( label( KEY ), PROP, DUPLICATED_VALUE ) )
            {
                while ( nodes.hasNext() )
                {
                    nodes.next().delete();
                }
            }
        }

        @Test
        public void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
        {
            // given
            long node1, node2;
            int foo, name;
            {
                DataWriteOperations statement = dataWriteOperationsInNewTransaction();
                // name is not unique for Foo in the existing data

                foo = statement.labelGetOrCreateForName( "Foo" );
                name = statement.propertyKeyGetOrCreateForName( "name" );

                long node = statement.nodeCreate();
                node1 = node;
                statement.nodeAddLabel( node, foo );
                statement.nodeSetProperty( node, Property.stringProperty( name, "foo" ) );

                node = statement.nodeCreate();
                statement.nodeAddLabel( node, foo );
                node2 = node;
                statement.nodeSetProperty( node, Property.stringProperty( name, "foo" ) );
                commit();
            }

            // when
            try
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                statement.uniquePropertyConstraintCreate( foo, name );

                fail( "expected exception" );
            }
            // then
            catch ( CreateConstraintFailureException ex )
            {
                assertEquals( new UniquenessConstraint( foo, name ), ex.constraint() );
                Throwable cause = ex.getCause();
                assertThat( cause, instanceOf( ConstraintVerificationFailedKernelException.class ) );

                String expectedMessage = format(
                        "Multiple nodes with label `%s` have property `%s` = '%s':%n  node(%d)%n  node(%d)",
                        "Foo", "name", "foo", node1, node2 );
                String actualMessage = userMessage( (ConstraintVerificationFailedKernelException) cause );
                assertEquals( expectedMessage, actualMessage );
            }
        }

        @Test
        public void shouldCreateAnIndexToGoAlongWithAUniquePropertyConstraint() throws Exception
        {
            // when
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
                commit();
            }

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();
                assertEquals( asSet( new IndexDescriptor( typeId, propertyKeyId ) ),
                        asSet( statement.uniqueIndexesGetAll() ) );
            }
        }

        @Test
        public void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
        {
            // given
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
                assertEquals( asSet( new IndexDescriptor( typeId, propertyKeyId ) ),
                        asSet( statement.uniqueIndexesGetAll() ) );
            }

            // when
            rollback();

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();
                assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
                commit();
            }
        }

        @Test
        public void shouldNotDropUniquePropertyConstraintThatDoesNotExistWhenThereIsAPropertyExistenceConstraint()
                throws Exception
        {
            // given
            NodePropertyExistenceConstraint constraint;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                constraint = statement.nodePropertyExistenceConstraintCreate( typeId, propertyKeyId );
                commit();
            }

            // when
            try
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                statement.constraintDrop( new UniquenessConstraint( constraint.label(), constraint.propertyKey() ) );

                fail( "expected exception" );
            }
            // then
            catch ( DropConstraintFailureException e )
            {
                assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
            }
            finally
            {
                rollback();
            }

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();

                Iterator<NodePropertyConstraint> constraints =
                        statement.constraintsGetForLabelAndPropertyKey( typeId, propertyKeyId );

                assertEquals( constraint, single( constraints ) );
            }
        }

        @Test
        public void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
        {
            // when
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
            commit();

            // then
            SchemaStorage schema = new SchemaStorage( neoStore().getSchemaStore() );
            IndexRule indexRule = schema.indexRule( typeId, propertyKeyId );
            UniquePropertyConstraintRule constraintRule = schema.uniquenessConstraint( typeId, propertyKeyId );
            assertEquals( constraintRule.getId(), indexRule.getOwningConstraint().longValue() );
            assertEquals( indexRule.getId(), constraintRule.getOwnedIndex() );
        }

        @Test
        public void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
        {
            // given
            UniquenessConstraint constraint;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                constraint = statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
                assertEquals( asSet( new IndexDescriptor( typeId, propertyKeyId ) ),
                        asSet( statement.uniqueIndexesGetAll() ) );
                commit();
            }

            // when
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                statement.constraintDrop( constraint );
                commit();
            }

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();
                assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
                commit();
            }
        }

        private String userMessage( ConstraintVerificationFailedKernelException cause )
                throws TransactionFailureException
        {
            StatementTokenNameLookup lookup = new StatementTokenNameLookup( readOperationsInNewTransaction() );
            String actualMessage = cause.getUserMessage( lookup );
            commit();
            return actualMessage;
        }
    }

    public abstract static class AbstractConstraintCreationIT<Constraint extends PropertyConstraint>
            extends KernelIntegrationTest
    {
        protected static final String KEY = "Foo";
        protected static final String PROP = "bar";

        protected int typeId;
        protected int propertyKeyId;

        abstract int initializeLabelOrRelType( SchemaWriteOperations writeOps, String name ) throws KernelException;

        abstract Constraint createConstraint( SchemaWriteOperations writeOps, int type, int property ) throws Exception;

        abstract void createConstraintInRunningTx( GraphDatabaseService db, String type, String property );

        abstract Constraint newConstraintObject( int type, int property );

        abstract void dropConstraint( SchemaWriteOperations writeOps, Constraint constraint ) throws Exception;

        abstract void createOffendingDataInRunningTx( GraphDatabaseService db );

        abstract void removeOffendingDataInRunningTx( GraphDatabaseService db );


        @Before
        public void createKeys() throws Exception
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            this.typeId = initializeLabelOrRelType( statement, KEY );
            this.propertyKeyId = statement.propertyKeyGetOrCreateForName( PROP );
            commit();
        }

        @Test
        public void shouldBeAbleToStoreAndRetrieveConstraint() throws Exception
        {
            // given
            PropertyConstraint constraint;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                // when
                constraint = createConstraint( statement, typeId, propertyKeyId );

                // then
                assertEquals( constraint, single( statement.constraintsGetAll() ) );

                // given
                commit();
            }
            {
                ReadOperations statement = readOperationsInNewTransaction();

                // when
                Iterator<?> constraints = statement.constraintsGetAll();

                // then
                assertEquals( constraint, single( constraints ) );
            }
        }

        @Test
        public void shouldNotPersistConstraintCreatedInAbortedTransaction() throws Exception
        {
            // given
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                createConstraint( statement, typeId, propertyKeyId );

                // when
                rollback();
            }
            {
                ReadOperations statement = readOperationsInNewTransaction();

                // then
                Iterator<?> constraints = statement.constraintsGetAll();
                assertFalse( "should not have any constraints", constraints.hasNext() );
            }
        }

        @Test
        public void shouldNotStoreConstraintThatIsRemovedInTheSameTransaction() throws Exception
        {
            // given
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                Constraint constraint = createConstraint( statement, typeId, propertyKeyId );

                // when
                dropConstraint( statement, constraint );

                // then
                assertFalse( "should not have any constraints", statement.constraintsGetAll().hasNext() );

                // when
                commit();
            }
            {
                ReadOperations statement = readOperationsInNewTransaction();

                // then
                assertFalse( "should not have any constraints", statement.constraintsGetAll().hasNext() );
            }
        }

        @Test
        public void shouldDropConstraint() throws Exception
        {
            // given
            Constraint constraint;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                constraint = createConstraint( statement, typeId, propertyKeyId );
                commit();
            }

            // when
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                dropConstraint( statement, constraint );
                commit();
            }

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();

                // then
                assertFalse( "should not have any constraints", statement.constraintsGetAll().hasNext() );
            }
        }

        @Test
        public void shouldNotCreateConstraintThatAlreadyExists() throws Exception
        {
            // given
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                createConstraint( statement, typeId, propertyKeyId );
                commit();
            }

            // when
            try
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                createConstraint( statement, typeId, propertyKeyId );

                fail( "Should not have validated" );
            }
            // then
            catch ( AlreadyConstrainedException e )
            {
                // good
            }
        }

        @Test
        public void shouldNotRemoveConstraintThatGetsReAdded() throws Exception
        {
            // given
            Constraint constraint;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                constraint = createConstraint( statement, typeId, propertyKeyId );
                commit();
            }
            SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                // when
                dropConstraint( statement, constraint );
                createConstraint( statement, typeId, propertyKeyId );
                commit();
            }
            {
                ReadOperations statement = readOperationsInNewTransaction();

                // then
                assertEquals( singletonList( constraint ), asCollection( statement.constraintsGetAll() ) );
                schemaState.assertNotCleared( statement );
            }
        }

        @Test
        public void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
        {
            // given
            SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

            // when
            createConstraint( statement, typeId, propertyKeyId );
            commit();

            // then
            schemaState.assertCleared( readOperationsInNewTransaction() );
            rollback();
        }

        @Test
        public void shouldClearSchemaStateWhenConstraintIsDropped() throws Exception
        {
            // given
            Constraint constraint;
            SchemaStateCheck schemaState;
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
                constraint = createConstraint( statement, typeId, propertyKeyId );
                commit();

                schemaState = new SchemaStateCheck().setUp();
            }

            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                // when
                dropConstraint( statement, constraint );
                commit();
            }

            // then
            schemaState.assertCleared( readOperationsInNewTransaction() );
            rollback();
        }

        @Test
        public void shouldNotDropConstraintThatDoesNotExist() throws Exception
        {
            // given
            Constraint constraint = newConstraintObject( typeId, propertyKeyId );

            // when
            {
                SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();

                try
                {
                    dropConstraint( statement, constraint );
                    fail( "Should not have dropped constraint" );
                }
                catch ( DropConstraintFailureException e )
                {
                    assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
                }
                commit();
            }

            // then
            {
                ReadOperations statement = readOperationsInNewTransaction();
                assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
                commit();
            }
        }

        @Test
        public void shouldNotLeaveAnyStateBehindAfterFailingToCreateConstraint() throws Exception
        {
            // given
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( Collections.<ConstraintDefinition>emptyList(), asList( db.schema().getConstraints() ) );
                assertEquals( Collections.<IndexDefinition,Schema.IndexState>emptyMap(),
                        indexesWithState( db.schema() ) );
                createOffendingDataInRunningTx( db );

                tx.success();
            }

            // when
            try ( Transaction tx = db.beginTx() )
            {
                createConstraintInRunningTx( db, KEY, PROP );

                tx.success();
                fail( "expected failure" );
            }
            catch ( ConstraintViolationException e )
            {
                assertTrue( e.getMessage().startsWith( "Unable to create CONSTRAINT" ) );
            }

            // then
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( Collections.<ConstraintDefinition>emptyList(), asList( db.schema().getConstraints() ) );
                assertEquals( Collections.<IndexDefinition,Schema.IndexState>emptyMap(),
                        indexesWithState( db.schema() ) );
                tx.success();
            }
        }

        @Test
        public void shouldBeAbleToResolveConflictsAndRecreateConstraintAfterFailingToCreateItDueToConflict()
                throws Exception
        {
            // given
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( Collections.<ConstraintDefinition>emptyList(), asList( db.schema().getConstraints() ) );
                assertEquals( Collections.<IndexDefinition,Schema.IndexState>emptyMap(),
                        indexesWithState( db.schema() ) );
                createOffendingDataInRunningTx( db );

                tx.success();
            }

            // when
            try ( Transaction tx = db.beginTx() )
            {
                createConstraintInRunningTx( db, KEY, PROP );

                tx.success();
                fail( "expected failure" );
            }
            catch ( ConstraintViolationException e )
            {
                assertTrue( e.getMessage().startsWith( "Unable to create CONSTRAINT" ) );
            }

            try ( Transaction tx = db.beginTx() )
            {
                removeOffendingDataInRunningTx( db );
                tx.success();
            }

            // then - this should not fail
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            createConstraint( statement, typeId, propertyKeyId );
            commit();
        }

        private static Map<IndexDefinition,Schema.IndexState> indexesWithState( Schema schema )
        {
            Map<IndexDefinition,Schema.IndexState> result = new HashMap<>();
            for ( IndexDefinition definition : schema.getIndexes() )
            {
                result.put( definition, schema.getIndexState( definition ) );
            }
            return result;
        }

        private class SchemaStateCheck implements Function<String,Integer>
        {
            int invocationCount;

            @Override
            public Integer apply( String s )
            {
                invocationCount++;
                return Integer.parseInt( s );
            }

            public SchemaStateCheck setUp() throws TransactionFailureException
            {
                ReadOperations readOperations = readOperationsInNewTransaction();
                checkState( readOperations );
                commit();
                return this;
            }

            public void assertCleared( ReadOperations readOperations )
            {
                int count = invocationCount;
                checkState( readOperations );
                assertEquals( "schema state should have been cleared.", count + 1, invocationCount );
            }

            public void assertNotCleared( ReadOperations readOperations )
            {
                int count = invocationCount;
                checkState( readOperations );
                assertEquals( "schema state should not have been cleared.", count, invocationCount );
            }

            private SchemaStateCheck checkState( ReadOperations readOperations )
            {
                assertEquals( Integer.valueOf( 7 ), readOperations.schemaStateGetOrCreate( "7", this ) );
                return this;
            }
        }
    }
}
