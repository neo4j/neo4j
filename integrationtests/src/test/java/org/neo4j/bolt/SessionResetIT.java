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
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.VerboseTimeout;

import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.helpers.Exceptions.rootCause;
import static org.neo4j.helpers.NamedThreadFactory.daemon;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class SessionResetIT
{
    private static final int CSV_FILE_SIZE = 10_000;
    private static final int LOAD_CSV_BATCH_SIZE = 10;

    private static final String SHORT_QUERY_1 = "CREATE (n:Node {name: 'foo', occupation: 'bar'})";
    private static final String SHORT_QUERY_2 = "MATCH (n:Node {name: 'foo'}) RETURN count(n)";
    private static final String LONG_QUERY = "UNWIND range(0, 10000000) AS i CREATE (n:Node {idx: i}) DELETE n";

    private static final String LONG_PERIODIC_COMMIT_QUERY_TEMPLATE =
            "USING PERIODIC COMMIT 1 " +
            "LOAD CSV FROM '%s' AS line " +
            "UNWIND range(1, " + LOAD_CSV_BATCH_SIZE + ") AS index " +
            "CREATE (n:Node {id: index, name: line[0], occupation: line[1]})";

    private static final int STRESS_IT_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
    private static final long STRESS_IT_DURATION_MS = SECONDS.toMillis( 5 );
    private static final String[] STRESS_IT_QUERIES = {SHORT_QUERY_1, SHORT_QUERY_2, LONG_QUERY};

    private final VerboseTimeout timeout = VerboseTimeout.builder().withTimeout( 6, MINUTES ).build();
    private final Neo4jRule db = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.load_csv_file_url_root, "import" )
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .dumpLogsOnFailure( System.out );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( timeout ).around( db );

    private Driver driver;

    @Before
    public void setUp()
    {
        driver = GraphDatabase.driver( db.boltURI(), Config.build().withLogging( DEV_NULL_LOGGING ).toConfig() );
    }

    @After
    public void tearDown()
    {
        IOUtils.closeAllSilently( driver );
    }

    @Test
    public void shouldTerminateAutoCommitQuery() throws Exception
    {
        testQueryTermination( LONG_QUERY, true );
    }

    @Test
    public void shouldTerminateQueryInExplicitTransaction() throws Exception
    {
        testQueryTermination( LONG_QUERY, false );
    }

    /**
     * It is currently unsafe to terminate periodic commit query because it'll then be half-committed.
     */
    @Test
    public void shouldNotTerminatePeriodicCommitQueries() throws Exception
    {
        Future<Void> queryResult = runQueryInDifferentThreadAndResetSession( longPeriodicCommitQuery(), true );

        try
        {
            queryResult.get( 1, MINUTES );
        }
        catch ( ExecutionException ignore )
        {
        }
        assertDatabaseIsIdle();

        assertEquals( CSV_FILE_SIZE * LOAD_CSV_BATCH_SIZE, countNodes() );
    }

    @Test
    public void shouldTerminateAutoCommitQueriesRandomly() throws Exception
    {
        testRandomQueryTermination( true );
    }

    @Test
    public void shouldTerminateQueriesInExplicitTransactionsRandomly() throws Exception
    {
        testRandomQueryTermination( false );
    }

    private void testRandomQueryTermination( boolean autoCommit ) throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool( STRESS_IT_THREAD_COUNT, daemon( "test-worker" ) );
        Set<Session> runningSessions = newSetFromMap( new ConcurrentHashMap<>() );
        AtomicBoolean stop = new AtomicBoolean();
        List<Future<?>> futures = new ArrayList<>();

        for ( int i = 0; i < STRESS_IT_THREAD_COUNT; i++ )
        {
            futures.add( executor.submit( () ->
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while ( !stop.get() )
                {
                    runRandomQuery( autoCommit, random, runningSessions, stop );
                }
            } ) );
        }

        long deadline = System.currentTimeMillis() + STRESS_IT_DURATION_MS;
        while ( !stop.get() )
        {
            if ( System.currentTimeMillis() > deadline )
            {
                stop.set( true );
            }

            resetAny( runningSessions );

            MILLISECONDS.sleep( 30 );
        }

        driver.close();
        awaitAll( futures );
        assertDatabaseIsIdle();
    }

    private void runRandomQuery( boolean autoCommit, Random random, Set<Session> runningSessions, AtomicBoolean stop )
    {
        try
        {
            Session session = driver.session();
            runningSessions.add( session );
            try
            {
                String query = STRESS_IT_QUERIES[random.nextInt( STRESS_IT_QUERIES.length - 1 )];
                runQuery( session, query, autoCommit );
            }
            finally
            {
                runningSessions.remove( session );
                session.close();
            }
        }
        catch ( Throwable error )
        {
            if ( !stop.get() && !isAcceptable( error ) )
            {
                stop.set( true );
                throw error;
            }
            // else it is fine to receive some errors from the driver because
            // sessions are being reset concurrently by the main thread, driver can also be closed concurrently
        }
    }

    private void testQueryTermination( String query, boolean autoCommit ) throws Exception
    {
        Future<Void> queryResult = runQueryInDifferentThreadAndResetSession( query, autoCommit );

        try
        {
            queryResult.get( 10, SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertTrue( isTransactionTerminatedException( e.getCause() ) );
        }

        assertDatabaseIsIdle();
    }

    private Future<Void> runQueryInDifferentThreadAndResetSession( String query, boolean autoCommit ) throws Exception
    {
        AtomicReference<Session> sessionRef = new AtomicReference<>();

        Future<Void> queryResult = runAsync( () ->
        {
            try ( Session session = driver.session() )
            {
                sessionRef.set( session );
                runQuery( session, query, autoCommit );
            }
        } );

        await( () -> activeQueriesCount() == 1, 10, SECONDS );
        SECONDS.sleep( 1 ); // additionally wait a bit before resetting the session

        Session session = sessionRef.get();
        assertNotNull( session );
        session.reset();

        return queryResult;
    }

    private static void runQuery( Session session, String query, boolean autoCommit )
    {
        if ( autoCommit )
        {
            session.run( query ).consume();
        }
        else
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( query );
                tx.success();
            }
        }
    }

    private void assertDatabaseIsIdle() throws InterruptedException
    {
        assertEventually( "Wrong number of active queries", this::activeQueriesCount, is( 0L ), 10, SECONDS );
        assertEventually( "Wrong number of active transactions", this::activeTransactionsCount, is( 0L ), 10, SECONDS );
    }

    private long activeQueriesCount()
    {
        try ( Result result = db().execute( "CALL dbms.listQueries() YIELD queryId RETURN count(queryId) AS result" ) )
        {
            return (long) single( result ).get( "result" ) - 1; // do not count listQueries procedure invocation
        }
    }

    private long activeTransactionsCount()
    {
        DependencyResolver resolver = db().getDependencyResolver();
        KernelTransactions kernelTransactions = resolver.resolveDependency( KernelTransactions.class );
        return kernelTransactions.activeTransactions().size();
    }

    private long countNodes()
    {
        try ( Result result = db().execute( "MATCH (n) RETURN count(n) AS result" ) )
        {
            return (long) single( result ).get( "result" );
        }
    }

    private GraphDatabaseAPI db()
    {
        return (GraphDatabaseAPI) db.getGraphDatabaseService();
    }

    private static void resetAny( Set<Session> sessions )
    {
        sessions.stream().findAny().ifPresent( session ->
        {
            if ( sessions.remove( session ) )
            {
                resetSafely( session );
            }
        } );
    }

    private static void resetSafely( Session session )
    {
        try
        {
            if ( session.isOpen() )
            {
                session.reset();
            }
        }
        catch ( ClientException e )
        {
            if ( session.isOpen() )
            {
                throw e;
            }
            // else this thread lost race with close and it's fine
        }
    }

    private static boolean isAcceptable( Throwable error )
    {
        Throwable cause = rootCause( error );

        return isTransactionTerminatedException( cause ) ||
               cause instanceof ServiceUnavailableException ||
               cause instanceof ClientException ||
               cause instanceof ClosedChannelException;
    }

    private static boolean isTransactionTerminatedException( Throwable error )
    {
        return error instanceof TransientException &&
               error.getMessage().startsWith( "The transaction has been terminated" );
    }

    private String longPeriodicCommitQuery()
    {
        URI fileUri = createTmpCsvFile();
        return String.format( LONG_PERIODIC_COMMIT_QUERY_TEMPLATE, fileUri );
    }

    private URI createTmpCsvFile()
    {
        try
        {
            Path importDir = db.getConfig().get( GraphDatabaseSettings.load_csv_file_url_root ).toPath();
            Files.createDirectory( importDir );
            Path csvFile = Files.createTempFile( importDir, "test", ".csv" );
            Iterable<String> lines = range( 0, CSV_FILE_SIZE ).mapToObj( i -> "Foo-" + i + ", Bar-" + i )::iterator;
            return URI.create( "file:///" + Files.write( csvFile, lines ).getFileName() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static void awaitAll( List<Future<?>> futures ) throws Exception
    {
        for ( Future<?> future : futures )
        {
            assertNull( future.get( 1, MINUTES ) );
        }
    }
}
