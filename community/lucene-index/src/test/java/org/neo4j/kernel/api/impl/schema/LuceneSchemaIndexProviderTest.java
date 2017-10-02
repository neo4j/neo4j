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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.search.Query;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.reader.SimpleIndexReader;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Additional tests for stuff not already covered by {@link LuceneSchemaIndexProviderCompatibilitySuiteTest}
 */
public class LuceneSchemaIndexProviderTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory( getClass() );
    @Rule
    public final RandomRule rnd = new RandomRule();

    private File graphDbDir;
    private FileSystemAbstraction fs;
    private static final int propertKeyId = 1;
    private static final IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 1, propertKeyId );

    @Before
    public void setup()
    {
        fs = fileSystemRule.get();
        graphDbDir = testDir.graphDbDir();
    }

    @Test
    public void shouldFailToInvokePopulatorInReadOnlyMode() throws Exception
    {
        Config readOnlyConfig = Config.embeddedDefaults( stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fs, graphDbDir );
        expectedException.expect( UnsupportedOperationException.class );

        readOnlyIndexProvider.getPopulator( 1L, descriptor, new IndexSamplingConfig(
                readOnlyConfig ) );
    }

    @Test
    public void shouldCreateReadOnlyAccessorInReadOnlyMode() throws Exception
    {
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        createEmptySchemaIndex( directoryFactory );

        Config readOnlyConfig = Config.embeddedDefaults( stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                directoryFactory, fs, graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( readOnlyConfig, readOnlyIndexProvider );

        expectedException.expect( UnsupportedOperationException.class );
        onlineAccessor.drop();
    }

    @Test
    public void indexUpdateNotAllowedInReadOnlyMode() throws Exception
    {
        Config readOnlyConfig = Config.embeddedDefaults( stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
        LuceneSchemaIndexProvider readOnlyIndexProvider = getLuceneSchemaIndexProvider( readOnlyConfig,
                new DirectoryFactory.InMemoryDirectoryFactory(), fs, graphDbDir );

        expectedException.expect( UnsupportedOperationException.class );
        getIndexAccessor( readOnlyConfig, readOnlyIndexProvider ).newUpdater( IndexUpdateMode.ONLINE );
    }

    @Test
    public void exceptionMustIncludeQueryInformation() throws Exception
    {
        SimpleIndexReader simpleIndexReader = getFailingIndexReader();

        String propertyValue = "myPropertyValue";
        try
        {
            simpleIndexReader.query( IndexQuery.exact( 1, propertyValue ) );
            fail( "Should have failed" );
        }
        catch ( Throwable t )
        {
            assertThat( t.getMessage(), allOf( containsString( descriptor.toString() ), containsString( propertyValue ) ) );
        }
    }

    @Test
    public void readerMustToggleVerboseOnException() throws Exception
    {
        SimpleIndexReader simpleIndexReader = getFailingIndexReader();

        String propertyValue = "myPropertyValue";
        try
        {
            assertFalse( ToggleableInfoStream.isEnabled() );
            simpleIndexReader.query( IndexQuery.exact( 1, propertyValue ) );
            fail( "Should have failed" );
        }
        catch ( Throwable t )
        {   // ok
            assertTrue( ToggleableInfoStream.isEnabled() );
        }
    }

    @Test
    public void mustLogInfoStreamIfToggled() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Config config = Config.defaults();
        LuceneSchemaIndexProvider indexProvider =
                new LuceneSchemaIndexProvider( fs, DirectoryFactory.PERSISTENT, graphDbDir, logProvider, config, OperationalMode.single );

        // when
        IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( 1L, descriptor, new IndexSamplingConfig( config ) );
        logProvider.assertNoLogCallContaining( "Lucene:" );
        logProvider.assertNoLogCallContaining( "[main]:" );
        ToggleableInfoStream.toggle( true );
        try ( IndexUpdater indexUpdater = onlineAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            indexUpdater.process( IndexEntryUpdate.add( 1, descriptor, "value" ) );
        }

        // then
        logProvider.assertContainsLogCallContaining( "Lucene:" );
        logProvider.assertContainsLogCallContaining( "[main]:" );
    }

    @Test
    public void mustSnapshotIndexFilesOnException() throws Exception
    {
        // given
        Config config = Config.defaults();
        LuceneSchemaIndexProvider indexProvider =
                new LuceneSchemaIndexProvider( fs, DirectoryFactory.PERSISTENT, graphDbDir, NullLogProvider.getInstance(), config,
                        OperationalMode.single );
        long indexId = 1L;
        try ( IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( indexId, descriptor, new IndexSamplingConfig( config ) );
              IndexUpdater indexUpdater = onlineAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            indexUpdater.process( IndexEntryUpdate.add( 1, descriptor, 1 ) );
        }

        // when query index with failing query
        try ( IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( indexId, descriptor, new IndexSamplingConfig( config ) );
              SimpleIndexReader indexReader = (SimpleIndexReader) onlineAccessor.newReader() )
        {
            try
            {
                indexReader.query( (Query) null );
                fail( "Should have failed" );
            }
            catch ( RuntimeException e )
            {
                // Good, this failure should have resulted in a snapshot dump
            }
        }

        // then
        assertSnapshotDump( indexId );
    }

    @Test
    public void stopStartOnConcurrentReadLoad() throws Exception
    {
        Config config = Config.defaults();
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
        LuceneSchemaIndexProvider indexProvider =
                new LuceneSchemaIndexProvider( fs, DirectoryFactory.PERSISTENT, graphDbDir, NullLogProvider.getInstance(), config,
                        OperationalMode.single );
        long indexId = 1L;
        try ( IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( indexId, descriptor, new IndexSamplingConfig( config ) );
              IndexUpdater indexUpdater = onlineAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            indexUpdater.process( IndexEntryUpdate.add( 1, descriptor, 1 ) );
        }

        // Thread signaling
        AtomicReference<Exception> workerFailure = new AtomicReference<>();
        AtomicBoolean end = new AtomicBoolean();
        AtomicInteger workerCount = new AtomicInteger();

        // given
        int concurrentWorkers = Runtime.getRuntime().availableProcessors() - 1;
        ExecutorService executor = Executors.newFixedThreadPool( concurrentWorkers );
        try ( IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ) )
        {
            for ( int i = 0; i < concurrentWorkers; i++ )
            {
                SimpleIndexReader indexReader = (SimpleIndexReader) onlineAccessor.newReader();
                executor.submit( () ->
                {
                    workerCount.incrementAndGet();
                    while ( !end.get() )
                    {
                        try
                        {
                            indexReader.query( randomQuery() );
                        }
                        catch ( Exception e )
                        {
                            workerFailure.getAndAccumulate( e, suppress() );
                        }
                    }
                } );
            }
            // Await workers alive
            while ( workerCount.get() < concurrentWorkers )
            {
                Thread.sleep( 1 );
            }

            // when query fails
            SimpleIndexReader failingReader = (SimpleIndexReader) onlineAccessor.newReader();
            try
            {
                failingReader.query( (Query) null );
                fail( "Should have failed" );
            }
            catch ( RuntimeException e )
            {
                // good
            }

            // then
            end.set( true );
            executor.shutdown();
            executor.awaitTermination( 1, TimeUnit.MINUTES );
        }
        Exception exception = workerFailure.get();
        if ( exception != null )
        {
            exception.printStackTrace();
        }
        assertSnapshotDump( indexId );
    }

    @Test
    public void stopStartOnConcurrentWriteLoad() throws Exception
    {
        Config config = Config.defaults();
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
        LuceneSchemaIndexProvider indexProvider =
                new LuceneSchemaIndexProvider( fs, DirectoryFactory.PERSISTENT, graphDbDir, NullLogProvider.getInstance(), config,
                        OperationalMode.single );
        long indexId = 1L;
        try ( IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( indexId, descriptor, new IndexSamplingConfig( config ) );
              IndexUpdater indexUpdater = onlineAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            indexUpdater.process( IndexEntryUpdate.add( 1, descriptor, 1 ) );
        }

        // Thread signaling
        AtomicInteger workerFailureCount = new AtomicInteger();
        AtomicBoolean end = new AtomicBoolean();
        AtomicInteger workerCount = new AtomicInteger();
        AtomicInteger nodeId = new AtomicInteger();

        // given
        int concurrentWorkers = Runtime.getRuntime().availableProcessors() - 1;
        ExecutorService executor = Executors.newFixedThreadPool( concurrentWorkers );
        try ( IndexAccessor onlineAccessor = indexProvider.getOnlineAccessor( indexId, descriptor, samplingConfig ) )
        {
            for ( int i = 0; i < concurrentWorkers; i++ )
            {
                executor.submit( () ->
                {
                    workerCount.incrementAndGet();
                    while ( !end.get() )
                    {
                        try ( IndexUpdater indexUpdater = onlineAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
                        {
                            indexUpdater.process( IndexEntryUpdate.add( nodeId.getAndIncrement(), descriptor, rnd.nextInt() ) );
                        }
                        catch ( Exception e )
                        {
                            workerFailureCount.incrementAndGet();
                        }
                    }
                } );
            }
            // Await workers alive
            while ( workerCount.get() < concurrentWorkers )
            {
                Thread.sleep( 1 );
            }

            // when query fails
            SimpleIndexReader failingReader = (SimpleIndexReader) onlineAccessor.newReader();
            try
            {
                failingReader.query( (Query) null );
                fail( "Should have failed" );
            }
            catch ( RuntimeException e )
            {
                // good
            }

            // this guy should succeed
            try ( IndexUpdater indexUpdater = onlineAccessor.newUpdater( IndexUpdateMode.ONLINE ) )
            {
                indexUpdater.process( IndexEntryUpdate.add( nodeId.getAndIncrement(), descriptor, rnd.nextInt() ) );
            }

            // then
            end.set( true );
            executor.shutdown();
            executor.awaitTermination( 1, TimeUnit.MINUTES );
        }
        assertSnapshotDump( indexId );
    }

    private Query randomQuery()
    {
        return LuceneDocumentStructure.newSeekQuery( rnd.nextInt() );
    }

    private BinaryOperator<Exception> suppress()
    {
        return ( current, given ) ->
        {
            if ( current == null )
            {
                return given;
            }
            current.addSuppressed( given );
            return current;
        };
    }

    private void assertSnapshotDump( long indexId )
    {
        File dumpDir = new File( graphDbDir, "dump_" + indexId );
        assertTrue( dumpDir.exists() );
        printDir( dumpDir );

        // Assert commit folders
        File indexDumpDir = new File( dumpDir, "index" );
        assertTrue( indexDumpDir.exists() );
        assertTrue( indexDumpDir.isDirectory() );
        File[] indexFiles = indexDumpDir.listFiles();
        assertNotNull( indexFiles );
        assertTrue( indexFiles.length > 0 );
    }

    private void printDir( File file )
    {
        printRecursive( "", file );
    }

    private void printRecursive( String indent, File file )
    {
        if ( file.isDirectory() )
        {
            System.out.println( indent + file );
            Arrays.stream( file.listFiles() ).forEach( f -> printRecursive( indent + "  ", f ) );
        }
        else if ( file.isFile() )
        {
            System.out.println( indent + file );
        }
    }

    @Test
    public void stopStartIntegrationTest() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( graphDbDir );
        Label label = Label.label( "label" );
        String propKey = "propKey";

        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool( processors );
        AtomicBoolean end = new AtomicBoolean();
        AtomicBoolean workerPrintStackTrace = new AtomicBoolean();
        AtomicInteger workerCount = new AtomicInteger();
        AtomicInteger workerFailureReadCount = new AtomicInteger();
        AtomicInteger workerFailureWriteCount = new AtomicInteger();
        AtomicInteger workerSuccessReadCount = new AtomicInteger();
        AtomicInteger workerSuccessWriteCount = new AtomicInteger();
        int operationsPerTx = 10;

        try
        {
            // Index
            createIndex( db, label, propKey );

            for ( int i = 0; i < 10; i++ )
            {
                try ( Transaction transaction = db.beginTx() ) {

                    randomIndexWrite( db, label, propKey );
                    transaction.success();
                }
            }

            // Concurrent reads and writes
            for ( int i = 0; i < processors; i++ )
            {
                executor.submit( () ->
                {
                    workerCount.incrementAndGet();
                    try
                    {
                        while ( !end.get() )
                        {
                            boolean success = true;
                            boolean write = rnd.nextBoolean();
                            try ( Transaction tx = db.beginTx() )
                            {
                                if ( write )
                                {
                                    // write
                                    randomIndexWrite( db, label, propKey );
                                }
                                else
                                {
                                    // read
                                    randomIndexRead( db, label, propKey );
                                }
                                tx.success();
                            }
                            catch ( Exception e )
                            {
                                success = false;
                                if ( workerPrintStackTrace.get() )
                                {
                                    e.printStackTrace();
                                }
                            }
                            if ( success )
                            {
                                if ( write )
                                {
                                    workerSuccessWriteCount.incrementAndGet();
                                }
                                else
                                {
                                    workerSuccessReadCount.incrementAndGet();
                                }
                            }
                            else
                            {
                                if ( write )
                                {
                                    workerFailureWriteCount.incrementAndGet();
                                }
                                else
                                {
                                    workerFailureReadCount.incrementAndGet();
                                }
                            }
                        }
                    }
                    finally
                    {
                        workerCount.decrementAndGet();
                    }
                } );
            }

            // Crash query
            try ( Transaction tx = db.beginTx() )
            {
                ReadOperations readOperations = readOperations( db );
                int propKeyId = readOperations.propertyKeyGetForName( propKey );
                IndexDescriptor descriptor = indexDescriptorFor( readOperations, label, propKey );
                try
                {
                    readOperations.indexQuery( descriptor, IndexQuery.fail( propKeyId ) );
                    fail( "Should have failed" );
                }
                catch ( Exception e )
                {   // good
                }
                tx.success();
            }

            // End
            end.set( true );
            executor.shutdown();
            executor.awaitTermination( 1, TimeUnit.MINUTES );
            while ( workerCount.get() > 0 )
            {
                Thread.sleep( 1 );
            }

            // Read and write to verify system is still alive
            try ( Transaction tx = db.beginTx() )
            {
                randomIndexRead( db, label, propKey );
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                randomIndexWrite( db, label, propKey );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }

    private ReadOperations readOperations( GraphDatabaseService db )
    {
        Statement statement =
                ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).get();
        return statement.readOperations();
    }

    private IndexDescriptor indexDescriptorFor( ReadOperations readOperations, Label label, String propKey )
    {
        int labelId = readOperations.labelGetForName( label.name() );
        int propKeyId = readOperations.propertyKeyGetForName( propKey );
        return IndexDescriptorFactory.forLabel( labelId, propKeyId );
    }

    private void randomIndexWrite( GraphDatabaseService db, Label label, String propKey )
    {
        db.createNode( label ).setProperty( propKey, randomPropertyValue() );
    }

    private void randomIndexRead( GraphDatabaseService db, Label label, String propKey )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label, propKey, randomPropertyValue() ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().getId();
            }
        }
    }

    private Object randomPropertyValue()
    {
        if ( rnd.nextBoolean() )
        {
            return "string";
        }
        else
        {
            return 1;
        }
    }

    private void createIndex( GraphDatabaseService db, Label label, String propKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition indexDefinition = db.schema().indexFor( label ).on( propKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private File luceneDir( File graphDbDir )
    {
        return FileUtils.path( graphDbDir, "schema", "index", "lucene" );
    }

    private SimpleIndexReader getFailingIndexReader()
    {
        Config config = Config.defaults();
        TaskCoordinator whateverTaskCoordinator = new TaskCoordinator( 10, TimeUnit.MILLISECONDS );
        IndexSamplingConfig whateverIndexSamplingConfig = new IndexSamplingConfig( config );
        return new SimpleIndexReader( null /* should cause NPE*/, descriptor, whateverIndexSamplingConfig, whateverTaskCoordinator, null );
    }

    private void createEmptySchemaIndex( DirectoryFactory directoryFactory ) throws IOException
    {
        Config config = Config.defaults();
        LuceneSchemaIndexProvider indexProvider = getLuceneSchemaIndexProvider( config, directoryFactory, fs,
                graphDbDir );
        IndexAccessor onlineAccessor = getIndexAccessor( config, indexProvider );
        onlineAccessor.flush();
        onlineAccessor.close();
    }

    private IndexAccessor getIndexAccessor( Config readOnlyConfig, LuceneSchemaIndexProvider indexProvider )
            throws IOException
    {
        return indexProvider.getOnlineAccessor( 1L, descriptor, new IndexSamplingConfig( readOnlyConfig ) );
    }

    private LuceneSchemaIndexProvider getLuceneSchemaIndexProvider( Config config, DirectoryFactory directoryFactory,
            FileSystemAbstraction fs, File graphDbDir )
    {
        return new LuceneSchemaIndexProvider(
                fs, directoryFactory, graphDbDir, NullLogProvider.getInstance(), config, OperationalMode.single );
    }
}
