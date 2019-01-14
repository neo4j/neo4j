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
package org.neo4j;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.RawPayload;
import org.neo4j.test.server.HTTP.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.NamedThreadFactory.named;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.withBaseUri;

@RunWith( Parameterized.class )
public class TransactionTerminationIT
{
    private static final Label LABEL = Label.label( "Foo" );
    private static final String PROPERTY = "bar";

    @Parameter
    public String lockManagerName;

    private final CleanupRule cleanupRule = new CleanupRule();
    private final ClusterRule clusterRule = new ClusterRule()
            .withCluster( clusterOfSize( 3 ) )
            .withSharedSetting( HaSettings.tx_push_factor, "2" )
            .withSharedSetting( HaSettings.lock_read_timeout, "1m" );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() )
            .around( cleanupRule )
            .around( clusterRule );

    @Parameters( name = "lockManager = {0}" )
    public static Iterable<Object[]> lockManagerNames()
    {
        return Arrays.asList( new Object[]{"forseti"}, new Object[]{"community"} );
    }

    @Test
    public void terminateSingleInstanceRestTransactionThatWaitsForLock() throws Exception
    {
        ServerControls server = cleanupRule.add( TestServerBuilders.newInProcessBuilder()
                .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE )
                .withConfig( GraphDatabaseFacadeFactory.Configuration.lock_manager, lockManagerName )
                .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .withConfig( ServerSettings.script_enabled, Settings.TRUE )
                .newServer() );

        GraphDatabaseService db = server.graph();
        HTTP.Builder http = withBaseUri( server.httpURI() );

        long value1 = 1L;
        long value2 = 2L;

        createNode( db );

        Response tx1 = startTx( http );
        Response tx2 = startTx( http );

        assertNumberOfActiveTransactions( 2, db );

        Response update1 = executeUpdateStatement( tx1, value1, http );
        assertThat( update1.status(), equalTo( 200 ) );
        assertThat( update1, containsNoErrors() );

        CountDownLatch latch = new CountDownLatch( 1 );
        Future<?> tx2Result = executeInSeparateThread( "tx2", () ->
        {
            latch.countDown();
            Response update2 = executeUpdateStatement( tx2, value2, http );
            assertTxWasTerminated( update2 );
        } );

        await( latch );
        sleepForAWhile();

        terminate( tx2, http );
        commit( tx1, http );

        Response update3 = executeUpdateStatement( tx2, value2, http );
        assertThat( update3.status(), equalTo( 404 ) );

        tx2Result.get( 1, TimeUnit.MINUTES );

        assertNodeExists( db, value1 );
    }

    @Test
    public void terminateSlaveTransactionThatWaitsForLockOnMaster() throws Exception
    {
        ClusterManager.ManagedCluster cluster = startCluster();

        String masterValue = "master";
        String slaveValue = "slave";

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        createNode( cluster );

        CountDownLatch masterTxStarted = new CountDownLatch( 1 );
        CountDownLatch masterTxCommit = new CountDownLatch( 1 );
        Future<?> masterTx = setPropertyInSeparateThreadAndWaitBeforeCommit( "masterTx", master, masterValue,
                masterTxStarted, masterTxCommit );

        await( masterTxStarted );

        AtomicReference<Transaction> slaveTxReference = new AtomicReference<>();
        CountDownLatch slaveTxStarted = new CountDownLatch( 1 );
        Future<?> slaveTx = setPropertyInSeparateThreadAndAttemptToCommit( "slaveTx", slave, slaveValue, slaveTxStarted,
                slaveTxReference );

        slaveTxStarted.await();
        sleepForAWhile();

        terminate( slaveTxReference );
        assertTxWasTerminated( slaveTx );

        masterTxCommit.countDown();
        assertNull( masterTx.get() );
        assertNodeExists( cluster, masterValue );
    }

    @Test
    public void terminateMasterTransactionThatWaitsForLockAcquiredBySlave() throws Exception
    {
        ClusterManager.ManagedCluster cluster = startCluster();

        String masterValue = "master";
        String slaveValue = "slave";

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        createNode( cluster );

        CountDownLatch slaveTxStarted = new CountDownLatch( 1 );
        CountDownLatch slaveTxCommit = new CountDownLatch( 1 );
        Future<?> slaveTx = setPropertyInSeparateThreadAndWaitBeforeCommit( "slaveTx", slave, slaveValue,
                slaveTxStarted, slaveTxCommit );

        await( slaveTxStarted );

        AtomicReference<Transaction> masterTxReference = new AtomicReference<>();
        CountDownLatch masterTxStarted = new CountDownLatch( 1 );
        Future<?> masterTx = setPropertyInSeparateThreadAndAttemptToCommit( "masterTx", master, masterValue,
                masterTxStarted, masterTxReference );

        masterTxStarted.await();
        sleepForAWhile();

        terminate( masterTxReference );
        assertTxWasTerminated( masterTx );

        slaveTxCommit.countDown();
        assertNull( slaveTx.get() );
        assertNodeExists( cluster, slaveValue );
    }

    private static void createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL );
            tx.success();
        }
    }

    private void createNode( ClusterManager.ManagedCluster cluster )
    {
        createNode( cluster.getMaster() );
        cluster.sync();
    }

    private static void assertNodeExists( GraphDatabaseService db, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = findNode( db );
            assertTrue( node.hasProperty( PROPERTY ) );
            assertEquals( value, node.getProperty( PROPERTY ) );
            tx.success();
        }
    }

    private static void assertNodeExists( ClusterManager.ManagedCluster cluster, Object value )
    {
        cluster.sync();
        assertNodeExists( cluster.getMaster(), value );
    }

    private static Node findNode( GraphDatabaseService db )
    {
        return single( db.findNodes( LABEL ) );
    }

    private static Response startTx( HTTP.Builder http )
    {
        Response tx = http.POST( "db/data/transaction" );
        assertThat( tx.status(), equalTo( 201 ) );
        assertThat( tx, containsNoErrors() );
        return tx;
    }

    private static void commit( Response tx, HTTP.Builder http ) throws JsonParseException
    {
        http.POST( tx.stringFromContent( "commit" ) );
    }

    private static void terminate( Response tx, HTTP.Builder http )
    {
        http.DELETE( tx.location() );
    }

    private void terminate( AtomicReference<Transaction> txReference )
    {
        Transaction tx = txReference.get();
        assertNotNull( tx );
        tx.terminate();
    }

    private static Response executeUpdateStatement( Response tx, long value, HTTP.Builder http )
    {
        String updateQuery = "MATCH (n:" + LABEL + ") SET n." + PROPERTY + "=" + value;
        RawPayload json = quotedJson( "{'statements': [{'statement':'" + updateQuery + "'}]}" );
        return http.POST( tx.location(), json );
    }

    private static void assertNumberOfActiveTransactions( int expectedCount, GraphDatabaseService db )
            throws InterruptedException
    {
        ThrowingSupplier<Integer,RuntimeException> txCount = () -> activeTxCount( db );
        assertEventually( "Wrong active tx count", txCount, equalTo( expectedCount ), 1, TimeUnit.MINUTES );
    }

    private static int activeTxCount( GraphDatabaseService db )
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        KernelTransactions kernelTransactions = resolver.resolveDependency( KernelTransactions.class );
        return kernelTransactions.activeTransactions().size();
    }

    private static void assertTxWasTerminated( Response txResponse )
    {
        assertEquals( 200, txResponse.status() );
        assertThat( txResponse, hasErrors( Status.Statement.ExecutionFailed ) );
        assertThat( txResponse.rawContent(), containsString( LockClientStoppedException.class.getSimpleName() ) );
    }

    private void assertTxWasTerminated( Future<?> txFuture ) throws InterruptedException
    {
        try
        {
            txFuture.get();
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( TransactionTerminatedException.class ) );
        }
    }

    private static void sleepForAWhile() throws InterruptedException
    {
        Thread.sleep( 2_000 );
    }

    private static void await( CountDownLatch latch )
    {
        try
        {
            assertTrue( latch.await( 2, TimeUnit.MINUTES ) );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static Future<?> setPropertyInSeparateThreadAndWaitBeforeCommit( String threadName, GraphDatabaseService db,
            Object value, CountDownLatch txStarted, CountDownLatch txCommit )
    {
        return executeInSeparateThread( threadName, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = findNode( db );
                node.setProperty( PROPERTY, value );
                txStarted.countDown();
                await( txCommit );
                tx.success();
            }
        } );
    }

    private static Future<?> setPropertyInSeparateThreadAndAttemptToCommit( String threadName,
            GraphDatabaseService db, Object value, CountDownLatch txStarted, AtomicReference<Transaction> txReference )
    {
        return executeInSeparateThread( threadName, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                txReference.set( tx );
                Node node = findNode( db );
                txStarted.countDown();
                node.setProperty( PROPERTY, value );
                tx.success();
            }
        } );
    }

    private static Future<?> executeInSeparateThread( String threadName, Runnable runnable )
    {
        return Executors.newSingleThreadExecutor( named( threadName ) ).submit( runnable );
    }

    private ClusterManager.ManagedCluster startCluster()
    {
        clusterRule.withSharedSetting( GraphDatabaseFacadeFactory.Configuration.lock_manager, lockManagerName );

        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        cluster.await( ClusterManager.allSeesAllAsAvailable() );
        return cluster;
    }
}
