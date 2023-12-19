/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.ConstraintHaIT.NodePropertyExistenceConstraintHaIT;
import org.neo4j.kernel.api.ConstraintHaIT.RelationshipPropertyExistenceConstraintHaIT;
import org.neo4j.kernel.api.ConstraintHaIT.UniquenessConstraintHaIT;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.coreapi.schema.NodePropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.RelationshipPropertyExistenceConstraintDefinition;
import org.neo4j.kernel.impl.coreapi.schema.UniquenessConstraintDefinition;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

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
            db.execute( String.format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT exists(n.`%s`)", type, value ) );
        }

        @Override
        protected ConstraintDefinition getConstraint( GraphDatabaseService db, String type, String value )
        {
            return Iterables.singleOrNull( db.schema().getConstraints( label( type ) ) );
        }

        @Override
        protected IndexDefinition getIndex( GraphDatabaseService db, String type, String value )
        {
            return null;
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
            db.execute( String.format( "CREATE CONSTRAINT ON ()-[r:`%s`]-() ASSERT exists(r.`%s`)", type, value ) );
        }

        @Override
        protected ConstraintDefinition getConstraint( GraphDatabaseService db, String type, String value )
        {
            return Iterables.singleOrNull( db.schema().getConstraints( withName( type ) ) );
        }

        @Override
        protected IndexDefinition getIndex( GraphDatabaseService db, String type, String value )
        {
            return null;
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
            db.execute( String.format( "CREATE CONSTRAINT ON (n:`%s`) ASSERT n.`%s` IS UNIQUE", type, value ) );
        }

        @Override
        protected ConstraintDefinition getConstraint( GraphDatabaseService db, String type, String value )
        {
            return Iterables.singleOrNull( db.schema().getConstraints( Label.label( type ) ) );
        }

        @Override
        protected IndexDefinition getIndex( GraphDatabaseService db, String type, String value )
        {
            return Iterables.singleOrNull( db.schema().getIndexes( Label.label( type ) ) );
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
        protected Class<? extends ConstraintDefinition> constraintDefinitionClass()
        {
            return UniquenessConstraintDefinition.class;
        }
    }

    public abstract static class AbstractConstraintHaIT
    {
        @Rule
        public ClusterRule clusterRule = new ClusterRule()
                .withSharedSetting( HaSettings.read_timeout, "4000s" );

        private static final String TYPE = "Type";
        private static final String PROPERTY_KEY = "name";

        // These type/key methods are due to the ClusterRule being a ClassRule so that one cluster
        // is used for all the tests, and so they need to have each their own constraint
        protected String type( int id )
        {
            return TYPE + "_" + getClass().getSimpleName() + "_" + id;
        }

        protected String key( int id )
        {
            return PROPERTY_KEY + "_" + getClass().getSimpleName() + "_" + id;
        }

        protected abstract void createConstraint( GraphDatabaseService db, String type, String value );

        /**
         * @return {@code null} if it has been dropped.
         */
        protected abstract ConstraintDefinition getConstraint( GraphDatabaseService db, String type, String value );

        /**
         * @return {@code null} if it has been dropped.
         */
        protected abstract IndexDefinition getIndex( GraphDatabaseService db, String type, String value );

        protected abstract void createEntityInTx( GraphDatabaseService db, String type, String propertyKey,
                String value );

        protected abstract void createConstraintViolation( GraphDatabaseService db, String type, String propertyKey,
                String value );

        protected abstract Class<? extends ConstraintDefinition> constraintDefinitionClass();

        @Test
        public void shouldCreateConstraintOnMaster()
        {
            // given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            HighlyAvailableGraphDatabase master = cluster.getMaster();
            String type = type( 0 );
            String key = key( 0 );

            // when
            try ( Transaction tx = master.beginTx() )
            {
                createConstraint( master, type, key );
                tx.success();
            }

            cluster.sync();

            // then
            for ( HighlyAvailableGraphDatabase clusterMember : cluster.getAllMembers() )
            {
                try ( Transaction tx = clusterMember.beginTx() )
                {
                    ConstraintDefinition constraint = getConstraint( clusterMember, type, key );
                    validateLabelOrRelationshipType( constraint, type );
                    assertEquals( key, single( constraint.getPropertyKeys() ) );
                    tx.success();
                }
            }
        }

        @Test
        public void shouldNotBePossibleToCreateConstraintsDirectlyOnSlaves()
        {
            // given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            String type = type( 1 );
            String key = key( 1 );

            // when
            try ( Transaction ignored = slave.beginTx() )
            {
                createConstraint( slave, type, key );
                fail( "We expected to not be able to create a constraint on a slave in a cluster." );
            }
            catch ( QueryExecutionException e )
            {
                assertThat( Exceptions.rootCause( e ), instanceOf( InvalidTransactionTypeKernelException.class ) );
            }
        }

        @Test
        public void shouldRemoveConstraints()
        {
            // given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            HighlyAvailableGraphDatabase master = cluster.getMaster();
            String type = type( 2 );
            String key = key( 2 );

            long constraintCountBefore;
            long indexCountBefore;
            try ( Transaction tx = master.beginTx() )
            {
                constraintCountBefore = count( master.schema().getConstraints() );
                indexCountBefore = count( master.schema().getIndexes() );
                createConstraint( master, type, key );
                tx.success();
            }
            cluster.sync();

            // and given I have some data for the constraint
            createEntityInTx( cluster.getAnySlave(), type, key, "Foo" );

            // when
            try ( Transaction tx = master.beginTx() )
            {
                getConstraint( master, type, key ).drop();
                tx.success();
            }
            cluster.sync();

            // then the constraint should be gone, and not be enforced anymore
            for ( HighlyAvailableGraphDatabase clusterMember : cluster.getAllMembers() )
            {
                try ( Transaction tx = clusterMember.beginTx() )
                {
                    assertNull( getConstraint( clusterMember, type, key ) );
                    assertNull( getIndex( clusterMember, type, key ) );
                    createConstraintViolation( clusterMember, type, key, "Foo" );
                    tx.success();
                }
            }
        }

        @Test
        public void newSlaveJoiningClusterShouldNotAcceptOperationsUntilConstraintIsOnline() throws Throwable
        {
            // Given
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            String type = type( 4 );
            String key = key( 4 );

            HighlyAvailableGraphDatabase master = cluster.getMaster();

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            File slaveStoreDirectory = cluster.getStoreDir( slave );

            // Crash the slave
            ClusterManager.RepairKit shutdownSlave = cluster.shutdown( slave );
            deleteRecursively( slaveStoreDirectory );

            try ( Transaction tx = master.beginTx() )
            {
                createConstraint( master, type, key );
                tx.success();
            }

            // When
            slave = shutdownSlave.repair();

            // Then
            try ( Transaction ignored = slave.beginTx() )
            {
                ConstraintDefinition definition = getConstraint( slave, type, key );
                assertThat( definition, instanceOf( constraintDefinitionClass() ) );
                assertThat( single( definition.getPropertyKeys() ), equalTo( key ) );
                validateLabelOrRelationshipType( definition, type );
            }
        }

        private static void validateLabelOrRelationshipType( ConstraintDefinition constraint, String type )
        {
            if ( constraint.isConstraintType( ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE ) )
            {
                assertEquals( type, constraint.getRelationshipType().name() );
            }
            else
            {
                assertEquals( type, constraint.getLabel().name() );
            }
        }
    }
}
