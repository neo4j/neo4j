/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappings;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.SilentExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.Writer;
import org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;
import org.neo4j.unsafe.impl.batchimport.store.io.SimplePool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.SYNCHRONOUS;

@RunWith(Parameterized.class)
public class ParallelBatchImporterTest
{
    private static final long SEED = 12345L;
    private static final String[] LABELS = new String[]{"Person", "Guy"};
    private static final int NODE_COUNT = 100_000;
    private static final int RELATIONSHIP_COUNT = NODE_COUNT * 10;

    private static final Configuration config = new Configuration.Default()
    {
        @Override
        public int denseNodeThreshold()
        {
            return 30;
        }
    };

    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(
                new Object[]{"synchronous", SYNCHRONOUS},
                new Object[]{"slow-synchronous", synchronousSlowWriterFactory}
                // FIXME: disable these WriterFactories due to a race in IoQueue
//                ,
//                new Object[]{"io-queue", new IoQueue( config.numberOfIoThreads(), SYNCHRONOUS )},
//                new Object[]{"slow-io-queue", new IoQueue( config.numberOfIoThreads(), synchronousSlowWriterFactory )}
        );
    }

    @Parameterized.Parameter(0)
    public String ignored; // needed to make Parametrized happy

    @Parameterized.Parameter(1)
    public WriterFactory writerFactory;

    @Test
    public void shouldImportCsvData() throws Exception
    {
        // GIVEN
        final BatchImporter inserter = new ParallelBatchImporter( directory.absolutePath(),
                new DefaultFileSystemAbstraction(), config, new DevNullLoggingService(),
                new SilentExecutionMonitor(), writerFactory );

        // WHEN
        inserter.doImport( nodes( NODE_COUNT ), relationships( RELATIONSHIP_COUNT, NODE_COUNT ), IdMappings.actual() );
        inserter.shutdown();

        // THEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            verifyData( NODE_COUNT, db );
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

    private static WriterFactory synchronousSlowWriterFactory = new WriterFactory()
    {
        @Override
        public Writer create( final StoreChannel channel, final Monitor monitor )
        {
            return new Writer()
            {
                final Writer delegate = SYNCHRONOUS.create( channel, monitor );

                @Override
                public void write( ByteBuffer data, long position, SimplePool<ByteBuffer> pool )
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
    };

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
                    private final Object[] properties = new Object[] {
                            "name", "Nisse " + cursor,
                            "age", 10,
                            "long-string", "OK here goes... a long string that will certainly end up in a dynamic record1234567890!@#$%^&*()_|",
                            "array", new long[] { 1234567890123L, 987654321987L, 123456789123L, 987654321987L }
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
                    private final Object[] properties = new Object[] {
                            "name", "Nisse " + cursor,
                            "age", 10,
                            "long-string", "OK here goes... a long string that will certainly end up in a dynamic record1234567890!@#$%^&*()_|",
                            "array", new long[] { 1234567890123L, 987654321987L, 123456789123L, 987654321987L }
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
