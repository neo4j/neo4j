/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.collection.pool.Pool;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappings;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.SilentExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache;
import org.neo4j.unsafe.impl.batchimport.store.io.IoQueue;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;

public class ParallelBatchImporterTest
{
    private static final long SEED = 12345L;
    protected static final String[] LABELS = new String[]{"Person", "Guy"};
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    private void shouldImportCsvData0( BatchingPageCache.WriterFactory delegateWriterFactory ) throws Exception
    {
        // GIVEN
        Configuration config = new Configuration.Default()
        {
            @Override
            public int denseNodeThreshold()
            {
                return 30;
            }
        };
        BatchImporter inserter = new ParallelBatchImporter( directory.absolutePath(),
                new DefaultFileSystemAbstraction(), config, new DevNullLoggingService(),
                new SilentExecutionMonitor(), new IoQueue( config.numberOfIoThreads(), delegateWriterFactory ) );

        // WHEN
        int nodeCount = 100_000;
        int relationshipCount = nodeCount * 10;
        inserter.doImport( nodes( nodeCount ),
                relationships( relationshipCount, nodeCount ),
                IdMappings.actual() );

        // THEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            verifyData( nodeCount, db );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
        assertConsistent( directory.absolutePath() );
    }

    private void assertConsistent( String storeDir ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService consistencyChecker = new ConsistencyCheckService();
        Result result = consistencyChecker.runFullConsistencyCheck( storeDir,
                new Config(), ProgressMonitorFactory.NONE, StringLogger.DEV_NULL );
        assertTrue( "Database contains inconsistencies, there should be a report in " + storeDir,
                result.isSuccessful() );
    }

    @Test
    public void shouldImportCsvData() throws Exception
    {
        shouldImportCsvData0( SYNCHRONOUS );
    }

    @Test
    public void shouldImportCsvDataWhenWritesAreSlow() throws Exception
    {
        shouldImportCsvData0( new BatchingPageCache.WriterFactory()
        {
            @Override
            public BatchingPageCache.Writer create( final File file, final StoreChannel channel, final Monitor monitor )
            {
                return new BatchingPageCache.Writer()
                {
                    final BatchingPageCache.Writer delegate = SYNCHRONOUS.create( file, channel, monitor );

                    @Override
                    public void write( ByteBuffer data, long position, Pool<ByteBuffer> pool )
                            throws IOException
                    {
                        LockSupport.parkNanos( 50_000_000 ); // slowness comes from here
                        delegate.write( data, position, pool );

                    }
                };
            }

            @Override
            public void awaitEverythingWritten()
            {   // no-op
            }

            @Override
            public void shutdown()
            {   // no-op
            }
        } );
    }

    protected void verifyData( int nodeCount, GraphDatabaseService db )
    {
        // Verify that all the labels are in place
        Set<Label> expectedLabels = new HashSet<>();
        for ( String label : LABELS )
        {
            expectedLabels.add( DynamicLabel.label( label ) );
        }
        GlobalGraphOperations globalOps = GlobalGraphOperations.at( db );
        Set<Label> allLabels = Iterables.toSet( globalOps.getAllLabels() );
        assertEquals( allLabels, expectedLabels );

        // Sample some nodes for deeper inspection of their contents
        Random random = new Random();
        for ( int i = 0; i < nodeCount / 10; i++ )
        {
            Node node = db.getNodeById( random.nextInt( nodeCount ) );
            int count = count( node.getRelationships() );
            assertEquals( "For node " + node, count, node.getDegree() );
            for ( String key : node.getPropertyKeys() )
            {
                node.getProperty( key );
                Set<Label> actualLabels = Iterables.toSet( node.getLabels() );
                assertEquals( actualLabels, expectedLabels );
            }
        }

        // The label scan store should find all nodes since they all have the same labels
        Label firstLabel = DynamicLabel.label( LABELS[0] );
        ResourceIterable<Node> allNodesWithLabel =
                globalOps.getAllNodesWithLabel( firstLabel );
        assertEquals( count( allNodesWithLabel ), nodeCount );

        // All nodes also have the same age=10 property, so we should again find them all
        ResourceIterable<Node> foundNodes =
                db.findNodesByLabelAndProperty( firstLabel, "age", 10 );
        assertEquals( count( foundNodes ), nodeCount );
    }

    private static Iterable<InputRelationship> relationships( final long count, final long maxNodeId )
    {
        return new Iterable<InputRelationship>()
        {
            @Override
            public Iterator<InputRelationship> iterator()
            {
                return new PrefetchingIterator<InputRelationship>()
                {
                    private final Random random = new Random( SEED );
                    private int cursor;
                    private final Object[] properties = new Object[]{
                            "name", "Nisse " + cursor,
                            "age", 10,
                            "long-string", "OK here goes... a long string that will certainly end up in a dynamic " +
                            "record1234567890!@#$%^&*()_|",
                            "array", new long[]{1234567890123L, 987654321987L, 123456789123L, 987654321987L}
                    };

                    @Override
                    protected InputRelationship fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            try
                            {
                                return new InputRelationship( cursor, properties, null,
                                        Math.abs( random.nextLong() % maxNodeId ),
                                        Math.abs( random.nextLong() % maxNodeId ), "TYPE" + random.nextInt( 3 ), null );
                            }
                            finally
                            {
                                cursor++;
                            }
                        }
                        return null;
                    }
                };
            }
        };
    }

    private static Iterable<InputNode> nodes( final long count )
    {
        return new Iterable<InputNode>()
        {
            @Override
            public Iterator<InputNode> iterator()
            {
                return new PrefetchingIterator<InputNode>()
                {
                    private long cursor;
                    private final Object[] properties = new Object[]{
                            "name", "Nisse " + cursor,
                            "age", 10,
                            "long-string", "OK here goes... a long string that will certainly end up in a dynamic " +
                            "record1234567890!@#$%^&*()_|",
                            "array", new long[]{1234567890123L, 987654321987L, 123456789123L, 987654321987L}
                    };

                    @Override
                    protected InputNode fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            try
                            {
                                return new InputNode( cursor, properties, null, LABELS, null );
                            }
                            finally
                            {
                                cursor++;
                            }
                        }
                        return null;
                    }
                };
            }
        };
    }
}
