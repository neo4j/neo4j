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

import java.io.File;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.InvalidTransactionTypeException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.schema.PropertyUniqueConstraintDefinition;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

public class UniqueConstraintHaIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void shouldCreateUniqueConstraintOnMaster() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        // when
        try ( Transaction tx = master.beginTx() )
        {
            master.schema().constraintFor( label( "Label1" ) ).assertPropertyIsUnique( "key1" ).create();
            tx.success();
        }

        cluster.sync();

        // then
        for ( HighlyAvailableGraphDatabase clusterMember : cluster.getAllMembers() )
        {
            try ( Transaction tx = clusterMember.beginTx() )
            {
                ConstraintDefinition constraint =
                        single( clusterMember.schema().getConstraints( label( "Label1" ) ) );
                assertEquals( "key1", single( constraint.getPropertyKeys() ) );
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
            slave.schema().constraintFor( label( "Label1" ) ).assertPropertyIsUnique( "key1" ).create();
            fail( "We expected to not be able to create a constraint on a slave in a cluster." );
        }
        catch ( Exception e )
        {
            assertThat(e, instanceOf(InvalidTransactionTypeException.class));
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
            master.schema().constraintFor( label( "User" ) ).assertPropertyIsUnique( "name" ).create();
            tx.success();
        }
        cluster.sync();

        // and given I have some data for the constraint
        createUser( cluster.getAnySlave(), "Bob" );

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
                assertEquals( count(clusterMember.schema().getConstraints()), 0);
                assertEquals( count(clusterMember.schema().getIndexes()), 0);
                createUser( clusterMember, "Bob" );
                tx.success();
            }
        }
    }

    @Test
    public void shouldNotAllowOldUncommittedTransactionsToResumeAndViolateConstraint() throws Exception
    {
        // Given
        ClusterManager.ManagedCluster cluster = clusterRule.config(HaSettings.read_timeout, "4000s").startCluster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        ThreadToStatementContextBridge txBridge = slave.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

        // And given there is a user named bob
        createUser(master, "Bob");

        // And given that I begin a transaction that will create another user named bob
        slave.beginTx();
        slave.createNode( label("User") ).setProperty( "name", "Bob" );
        TopLevelTransaction slaveTx = txBridge.getTopLevelTransactionBoundToThisThread( true );
        txBridge.unbindTransactionFromCurrentThread();

        // When I create a constraint for unique user names
        try(Transaction tx = master.beginTx())
        {
            master.schema().constraintFor( label("User") ).assertPropertyIsUnique( "name" ).create();
            tx.success();
        }

        // Then the transaction started on the slave should fail on commit, with an integrity error
        txBridge.bindTransactionToCurrentThread( slaveTx );
        try
        {
            slaveTx.success();
            slaveTx.finish();
            fail( "Expected this commit to fail :(" );
        }
        catch( TransactionFailureException e )
        {
            assertThat(e.getCause().getCause(), instanceOf( org.neo4j.kernel.api.exceptions.TransactionFailureException.class ));
        }

        // And then both master and slave should keep working, accepting reads
        assertOneBob( master );
        cluster.sync();
        assertOneBob( slave );

        // And then I should be able to perform new write transactions, on both master and slave
        createUser( slave, "Steven" );
        createUser( master, "Caroline" );
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

        try(Transaction tx = master.beginTx())
        {
            master.schema().constraintFor( label("User") ).assertPropertyIsUnique( "name" ).create();
            tx.success();
        }

        // When
        slave = shutdownSlave.repair();

        // Then
        try( Transaction ignored = slave.beginTx() )
        {
            assertThat(single( slave.schema().getConstraints() ), instanceOf(PropertyUniqueConstraintDefinition.class));
            PropertyUniqueConstraintDefinition constraint =
                    (PropertyUniqueConstraintDefinition)single(slave.schema().getConstraints());
            assertThat(single(constraint.getPropertyKeys()), equalTo("name"));
            assertThat(constraint.getLabel(), equalTo(label("User")));
        }
    }

    private void createUser( HighlyAvailableGraphDatabase db, String name )
    {
        try(Transaction tx = db.beginTx())
        {
            db.createNode( label("User") ).setProperty( "name", name );
            tx.success();
        }
    }

    private void assertOneBob( HighlyAvailableGraphDatabase db)
    {
        try(Transaction tx = db.beginTx())
        {
            assertThat( asList( db.findNodes( label( "User" ), "name", "Bob" ) ).size(), equalTo(1));
            tx.success();
        }
    }
}
