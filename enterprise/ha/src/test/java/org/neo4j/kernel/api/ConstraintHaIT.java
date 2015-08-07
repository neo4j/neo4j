/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.File;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.InvalidTransactionTypeException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.schema.NodePropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.RelationshipPropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.UniquenessConstraintDefinition;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Suite.SuiteClasses;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;
import static org.neo4j.kernel.api.ConstraintHaIT.*;

@RunWith( Suite.class )
@SuiteClasses( {
        NodePropertyExistenceConstraintHaIT.class,
        RelationshipPropertyExistenceConstraintHaIT.class,
        UniquenessConstraintHaIT.class
} )
public class ConstraintHaIT
{
    public static class NodePropertyExistenceConstraintHaIT extends AbstractConstraintHaIT
    {
        @Override
        protected void createConstraint( GraphDatabaseService db, String type, String value )
        {
            db.schema().constraintFor( label( type ) ).assertPropertyExists( value ).create();
        }

        @Override
        protected void createEntityInTx( GraphDatabaseService db, String type, String propertyKey,
                String value )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( label( type ) ).setProperty( propertyKey, value );
                tx.success();
            }
        }

        @Override
        protected void createConstraintViolation( GraphDatabaseService db, String type, String propertyKey,
                String value )
        {
            db.createNode( label( type ) );
        }

        @Override
        protected void assertConstraintHolds( GraphDatabaseService db, String type, String propertyKey, String value )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ResourceIterator<Node> nodes = db.findNodes( label( type ) );
                while ( nodes.hasNext() )
                {
                    Node node = nodes.next();
                    assertTrue( node.hasProperty( propertyKey ) );
                }
                tx.success();
            }
        }

        @Override
        protected Class<? extends ConstraintDefinition> constraintDefinitionClass()
        {
            return NodePropertyExistenceConstraintDefinition.class;
        }
    }

    public static class RelationshipPropertyExistenceConstraintHaIT extends AbstractConstraintHaIT
    {
        @Override
        protected void createConstraint( GraphDatabaseService db, String type, String value )
        {
            db.schema().constraintFor( withName( type ) ).assertPropertyExists( value ).create();
        }

        @Override
        protected void createEntityInTx( GraphDatabaseService db, String type, String propertyKey, String value )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node start = db.createNode();
                Node end = db.createNode();
                Relationship relationship = start.createRelationshipTo( end, withName( type ) );
                relationship.setProperty( propertyKey, value );
                tx.success();
            }
        }

        @Override
        protected void createConstraintViolation( GraphDatabaseService db, String type, String propertyKey,
                String value )
        {
            Node start = db.createNode();
            Node end = db.createNode();
            start.createRelationshipTo( end, withName( type ) );
        }

        @Override
        protected void assertConstraintHolds( GraphDatabaseService db, String type, String propertyKey, String value )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
                {
                    if ( relationship.isType( withName( type ) ) )
                    {
                        assertTrue( relationship.hasProperty( propertyKey ) );
                    }
                }
                tx.success();
            }
        }

        @Override
        protected Class<? extends ConstraintDefinition> constraintDefinitionClass()
        {
            return RelationshipPropertyExistenceConstraintDefinition.class;
        }
    }

    public static class UniquenessConstraintHaIT extends AbstractConstraintHaIT
    {
        @Override
        protected void createConstraint( GraphDatabaseService db, String type, String value )
        {
            db.schema().constraintFor( label( type ) ).assertPropertyIsUnique( value ).create();
        }

        @Override
        protected void createEntityInTx( GraphDatabaseService db, String type, String propertyKey,
                String value )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( label( type ) ).setProperty( propertyKey, value );
                tx.success();
            }
        }

        @Override
        protected void createConstraintViolation( GraphDatabaseService db, String type, String propertyKey,
                String value )
        {
            db.createNode( label( type ) ).setProperty( propertyKey, value );
        }

        @Override
        protected void assertConstraintHolds( GraphDatabaseService db, String type, String propertyKey, String value )
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertEquals( 1, toList( db.findNodes( label( type ), propertyKey, value ) ).size() );
                tx.success();
            }
        }

        @Override
        protected Class<? extends ConstraintDefinition> constraintDefinitionClass()
        {
            return UniquenessConstraintDefinition.class;
        }
    }

    public abstract static class AbstractConstraintHaIT
    {
        private static final String TYPE = "Type";
        private static final String PROPERTY_KEY = "name";

        @Rule
        public ClusterRule clusterRule = new ClusterRule( getClass() );

        protected abstract void createConstraint( GraphDatabaseService db, String type, String value );

        protected abstract void createEntityInTx( GraphDatabaseService db, String type, String propertyKey,
                String value );

        protected abstract void createConstraintViolation( GraphDatabaseService db, String type, String propertyKey,
                String value );

        protected abstract void assertConstraintHolds( GraphDatabaseService db, String type, String propertyKey,
                String value );

        protected abstract Class<? extends ConstraintDefinition> constraintDefinitionClass();

        @Test
        public void shouldCreateConstraintOnMaster() throws Exception
        {
            // given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            HighlyAvailableGraphDatabase master = cluster.getMaster();

            // when
            try ( Transaction tx = master.beginTx() )
            {
                createConstraint( master, TYPE, PROPERTY_KEY );
                tx.success();
            }

            cluster.sync();

            // then
            for ( HighlyAvailableGraphDatabase clusterMember : cluster.getAllMembers() )
            {
                try ( Transaction tx = clusterMember.beginTx() )
                {
                    ConstraintDefinition constraint = single( clusterMember.schema().getConstraints() );
                    validateLabelOrRelationshipType( constraint );
                    assertEquals( PROPERTY_KEY, single( constraint.getPropertyKeys() ) );
                    tx.success();
                }
            }
        }

        @Test
        public void shouldNotBePossibleToCreateConstraintsDirectlyOnSlaves() throws Exception
        {
            // given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

            // when
            try ( Transaction ignored = slave.beginTx() )
            {
                createConstraint( slave, TYPE, PROPERTY_KEY );
                fail( "We expected to not be able to create a constraint on a slave in a cluster." );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( InvalidTransactionTypeException.class ) );
            }
        }

        @Test
        public void shouldRemoveConstraints() throws Exception
        {
            // given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            HighlyAvailableGraphDatabase master = cluster.getMaster();

            try ( Transaction tx = master.beginTx() )
            {
                createConstraint( master, TYPE, PROPERTY_KEY );
                tx.success();
            }
            cluster.sync();

            // and given I have some data for the constraint
            createEntityInTx( cluster.getAnySlave(), TYPE, PROPERTY_KEY, "Foo" );

            // when
            try ( Transaction tx = master.beginTx() )
            {
                single( master.schema().getConstraints() ).drop();
                tx.success();
            }
            cluster.sync();

            // then the constraint should be gone, and not be enforced anymore
            for ( HighlyAvailableGraphDatabase clusterMember : cluster.getAllMembers() )
            {
                try ( Transaction tx = clusterMember.beginTx() )
                {
                    assertEquals( count( clusterMember.schema().getConstraints() ), 0 );
                    assertEquals( count( clusterMember.schema().getIndexes() ), 0 );
                    createConstraintViolation( clusterMember, TYPE, PROPERTY_KEY, "Foo" );
                    tx.success();
                }
            }
        }

        @Test
        public void shouldNotAllowOldUncommittedTransactionsToResumeAndViolateConstraint() throws Exception
        {
            // Given
            ClusterManager.ManagedCluster cluster =
                    clusterRule.config( HaSettings.read_timeout, "4000s" ).startCluster();
            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            HighlyAvailableGraphDatabase master = cluster.getMaster();

            ThreadToStatementContextBridge txBridge = threadToStatementContextBridge( slave );

            // And given there is an entity with property
            createEntityInTx( master, TYPE, PROPERTY_KEY, "Foo" );

            // And given that I begin a transaction that will create a constraint violation
            slave.beginTx();
            createConstraintViolation( slave, TYPE, PROPERTY_KEY, "Foo" );
            TopLevelTransaction slaveTx = txBridge.getTopLevelTransactionBoundToThisThread( true );
            txBridge.unbindTransactionFromCurrentThread();

            // When I create a constraint
            try ( Transaction tx = master.beginTx() )
            {
                createConstraint( master, TYPE, PROPERTY_KEY );
                tx.success();
            }

            // Then the transaction started on the slave should fail on commit, with an integrity error
            txBridge.bindTransactionToCurrentThread( slaveTx );
            try
            {
                slaveTx.success();
                slaveTx.close();
                fail( "Expected this commit to fail :(" );
            }
            catch ( org.neo4j.graphdb.TransactionFailureException e )
            {
                assertThat(
                        e.getCause().getCause(),
                        instanceOf( org.neo4j.kernel.api.exceptions.TransactionFailureException.class )
                );
            }

            // And then both master and slave should keep working, accepting reads
            assertConstraintHolds( master, TYPE, PROPERTY_KEY, "Foo" );
            cluster.sync();
            assertConstraintHolds( slave, TYPE, PROPERTY_KEY, "Foo" );

            // And then I should be able to perform new write transactions, on both master and slave
            createEntityInTx( slave, TYPE, PROPERTY_KEY, "Bar" );
            createEntityInTx( master, TYPE, PROPERTY_KEY, "Baz" );
        }

        @Test
        public void newSlaveJoiningClusterShouldNotAcceptOperationsUntilConstraintIsOnline() throws Throwable
        {
            // Given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();

            HighlyAvailableGraphDatabase master = cluster.getMaster();

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            File slaveStoreDirectory = cluster.getStoreDir( slave );

            // Crash the slave
            ClusterManager.RepairKit shutdownSlave = cluster.shutdown( slave );
            deleteRecursively( slaveStoreDirectory );

            try ( Transaction tx = master.beginTx() )
            {
                createConstraint( master, TYPE, PROPERTY_KEY );
                tx.success();
            }

            // When
            slave = shutdownSlave.repair();

            // Then
            try ( Transaction ignored = slave.beginTx() )
            {
                ConstraintDefinition definition = single( slave.schema().getConstraints() );
                assertThat( definition, instanceOf( constraintDefinitionClass() ) );
                assertThat( single( definition.getPropertyKeys() ), equalTo( PROPERTY_KEY ) );
                validateLabelOrRelationshipType( definition );
            }
        }

        private static ThreadToStatementContextBridge threadToStatementContextBridge( HighlyAvailableGraphDatabase db )
        {
            DependencyResolver dependencyResolver = db.getDependencyResolver();
            return dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        }

        private static void validateLabelOrRelationshipType( ConstraintDefinition constraint )
        {
            if ( constraint.isConstraintType( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE ) )
            {
                assertEquals( TYPE, constraint.getRelationshipType().name() );
            }
            else
            {
                assertEquals( TYPE, constraint.getLabel().name() );
            }
        }
    }
}
