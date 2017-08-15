/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package schema;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.BatchingMultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.Randoms;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.BadCollector;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.SimpleInputIteratorWrapper;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.NONE;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;

/**
 * Idea is to test a {@link MultipleIndexPopulator} and {@link BatchingMultipleIndexPopulator} with a bunch of indexes,
 * some which can fail randomly.
 * Also updates are randomly streaming in during population. In the end all the indexes should have been populated
 * with correct data.
 */
public class MultipleIndexPopulationStressIT
{
    private static final String[] TOKENS = new String[]{"One", "Two", "Three", "Four"};
    private final TestDirectory directory = TestDirectory.testDirectory( getClass() );

    private final RandomRule random = new RandomRule();
    private final CleanupRule cleanup = new CleanupRule();
    private final RepeatRule repeat = new RepeatRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( random ).around( repeat ).around( directory )
                                                .around( cleanup ).around( fileSystemRule );

    @Test
    public void populateMultipleIndexWithSeveralNodesSingleThreaded() throws Exception
    {
        prepareAndRunTest( false, 10, TimeUnit.SECONDS.toMillis( 5 ) );
    }

    @Test
    public void populateMultipleIndexWithSeveralNodesMultiThreaded() throws Exception
    {
        prepareAndRunTest( true, 10, TimeUnit.SECONDS.toMillis( 5 ) );
    }

    @Test
    public void shouldPopulateMultipleIndexPopulatorsUnderStressSingleThreaded() throws Exception
    {
        readConfigAndRunTest( false );
    }

    @Test
    public void shouldPopulateMultipleIndexPopulatorsUnderStressMultiThreaded() throws Exception
    {
        int concurrentUpdatesQueueFlushThreshold = random.nextInt( 100, 5000 );
        FeatureToggles.set( BatchingMultipleIndexPopulator.class, BatchingMultipleIndexPopulator.QUEUE_THRESHOLD_NAME,
                concurrentUpdatesQueueFlushThreshold );
        try
        {
            readConfigAndRunTest( true );
        }
        finally
        {
            FeatureToggles.clear( BatchingMultipleIndexPopulator.class,
                    BatchingMultipleIndexPopulator.QUEUE_THRESHOLD_NAME );
        }
    }

    private void readConfigAndRunTest( boolean multiThreaded ) throws Exception
    {
        // GIVEN a database with random data in it
        int nodeCount = (int) Settings.parseLongWithUnit( System.getProperty( getClass().getName() + ".nodes", "200k" ) );
        long duration = TimeUtil.parseTimeMillis.apply( System.getProperty( getClass().getName() + ".duration", "5s" ) );
        prepareAndRunTest( multiThreaded, nodeCount, duration );
    }

    private void prepareAndRunTest( boolean multiThreaded, int nodeCount, long durationMillis ) throws Exception
    {
        createRandomData( nodeCount );
        long endTime = currentTimeMillis() + durationMillis;

        // WHEN/THEN run tests for at least the specified durationMillis
        for ( int i = 0; currentTimeMillis() < endTime; i++ )
        {
            runTest( nodeCount, i, multiThreaded );
        }
    }

    private void runTest( int nodeCount, int run, boolean multiThreaded ) throws Exception
    {
        // WHEN creating the indexes under stressful updates
        populateDbAndIndexes( nodeCount, multiThreaded );
        ConsistencyCheckService cc = new ConsistencyCheckService();
        Result result = cc.runFullConsistencyCheck( directory.graphDbDir(),
                Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" ),
                NONE, NullLogProvider.getInstance(), false );
        assertTrue( result.isSuccessful() );
        dropIndexes();
    }

    private void populateDbAndIndexes( int nodeCount, boolean multiThreaded ) throws InterruptedException
    {
        final GraphDatabaseService db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.multi_threaded_schema_index_population_enabled, multiThreaded + "" )
                .newGraphDatabase();
        try
        {
            createIndexes( db );
            final AtomicBoolean end = new AtomicBoolean();
            ExecutorService executor = cleanup.add( Executors.newCachedThreadPool() );
            for ( int i = 0; i < 10; i++ )
            {
                executor.submit( () ->
                {
                    Randoms random = new Randoms();
                    while ( !end.get() )
                    {
                        changeRandomNode( db, nodeCount, random );
                    }
                } );
            }

            while ( !indexesAreOnline( db ) )
            {
                Thread.sleep( 100 );
            }
            end.set( true );
            executor.shutdown();
            executor.awaitTermination( 10, SECONDS );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void dropIndexes()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.pagecache_memory, "8m" )
                .newGraphDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                index.drop();
            }
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private boolean indexesAreOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                switch ( db.schema().getIndexState( index ) )
                {
                case ONLINE:
                    break; // Good
                case POPULATING:
                    return false; // Still populating
                case FAILED:
                    fail( index + " entered failed state: " + db.schema().getIndexFailure( index ) );
                default:
                    throw new UnsupportedOperationException();
                }
            }
            tx.success();
        }
        return true;
    }

    /**
     * Create a bunch of indexes in a single transaction. This will have all the indexes being built
     * using a single store scan... and this is the gist of what we're testing.
     */
    private void createIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( String label : random.selection( TOKENS, 3, 3, false ) )
            {
                for ( String propertyKey : random.selection( TOKENS, 3, 3, false ) )
                {
                    db.schema().indexFor( Label.label( label ) ).on( propertyKey ).create();
                }
            }
            tx.success();
        }
    }

    private void changeRandomNode( GraphDatabaseService db, int nodeCount, Randoms random )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = random.random().nextInt( nodeCount );
            Node node = db.getNodeById( nodeId );
            Object[] keys = Iterables.asCollection( node.getPropertyKeys() ).toArray();
            String key = (String) random.among( keys );
            if ( random.random().nextFloat() < 0.1 )
            {   // REMOVE
                node.removeProperty( key );
            }
            else
            {   // CHANGE
                node.setProperty( key, randomPropertyValue( random.random() ) );
            }
            tx.success();
        }
        catch ( NotFoundException e )
        {   // It's OK, it happens if some other thread deleted that property in between us reading it and
            // removing or setting it
        }
    }

    private void createRandomData( int count ) throws IOException
    {
        Config config = Config.defaults();
        RecordFormats recordFormats =
                RecordFormatSelector.selectForConfig( config, NullLogProvider.getInstance() );
        BatchImporter importer = new ParallelBatchImporter( directory.graphDbDir(), fileSystemRule.get(),
                null, DEFAULT, NullLogService.getInstance(), ExecutionMonitors.invisible(), EMPTY, config, recordFormats );
        importer.doImport( new RandomDataInput( count ) );
    }

    protected Iterable<InputNode> randomNodes( int count )
    {
        return () -> new InputNodePrefetchingIterator( count );
    }

    private int randomPropertyValue( Random random )
    {
        return random.nextInt( 100 );
    }

    private class InputNodePrefetchingIterator extends PrefetchingIterator<InputNode>
    {
        private final int count;
        private int i;

        InputNodePrefetchingIterator( int count )
        {
            this.count = count;
        }

        @Override
        protected InputNode fetchNextOrNull()
        {
            if ( i >= count )
            {
                return null;
            }

            try
            {
                return new InputNode( "Nodes", i, i, (long) i, randomProperties(), null, randomLabels(),
                        null );
            }
            finally
            {
                i++;
            }
        }

        private String[] randomLabels()
        {
            return random.randoms().selection( TOKENS, 1, TOKENS.length, false );
        }

        private Object[] randomProperties()
        {
            String[] keys = random.randoms().selection( TOKENS, 1, TOKENS.length, false );
            Object[] result = new Object[keys.length * 2];
            int i = 0;
            for ( String key : keys )
            {
                result[i++] = key;
                result[i++] = randomPropertyValue( random.random() );
            }
            return result;
        }
    }

    private class RandomDataInput implements Input
    {
        private final int count;

        RandomDataInput( int count )
        {
            this.count = count;
        }

        @Override
        public InputIterable<InputRelationship> relationships()
        {
            return SimpleInputIteratorWrapper.wrap( "Empty", Collections.emptyList() );
        }

        @Override
        public InputIterable<InputNode> nodes()
        {
            return SimpleInputIteratorWrapper.wrap( "Nodes", randomNodes( count ) );
        }

        @Override
        public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
        {
            return IdMappers.actual();
        }

        @Override
        public IdGenerator idGenerator()
        {
            return IdGenerators.fromInput();
        }

        @Override
        public Collector badCollector()
        {
            try
            {
                return new BadCollector( fileSystemRule.get().openAsOutputStream(
                        new File( directory.graphDbDir(), "bad" ), false ), 0, 0 );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
