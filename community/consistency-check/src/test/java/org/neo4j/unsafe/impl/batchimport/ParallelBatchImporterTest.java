/**
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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.collection.pool.Pool;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.RandomRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.input.SimpleInputIterator;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.Writer;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.WriterFactory;
import org.neo4j.unsafe.impl.batchimport.store.io.IoQueue;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.function.Functions.constant;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators.fromInput;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators.startingFromTheBeginning;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers.actual;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers.strings;
import static org.neo4j.unsafe.impl.batchimport.staging.ProcessorAssignmentStrategies.eagerRandomSaturation;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;

@RunWith( Parameterized.class )
public class ParallelBatchImporterTest
{
    private static final String[] LABELS = new String[]{"Person", "Guy"};
    private static final int NODE_COUNT = 10_000;
    private static final int RELATIONSHIP_COUNT = NODE_COUNT*5;
    public final @Rule TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private static final Configuration config = new Configuration.Default()
    {
        @Override
        public int batchSize()
        {
            // Set to extra low to exercise the internals and IoQueue a bit more.
            return 100;
        }

        @Override
        public int denseNodeThreshold()
        {
            return 30;
        }

        @Override
        public int maxNumberOfProcessors()
        {
            // Let's really crank up the number of threads to try and flush out all and any parallelization issues.
            return random.nextIntBetween( Runtime.getRuntime().availableProcessors(), 100 );
        }
    };
    private final Function<Configuration,WriterFactory> writerFactory;
    private final InputIdGenerator inputIdGenerator;
    private final IdMapper idMapper;
    private final IdGenerator idGenerator;
    private final boolean multiPassIterators;

    @Parameterized.Parameters(name = "{0},{1},{2},{4}")
    public static Collection<Object[]> data()
    {
        return Arrays.<Object[]>asList(

                // synchronous I/O, actual node id input
                new Object[]{SYNCHRONOUS, new LongInputIdGenerator(), actual(), fromInput(), true},
                // synchronous I/O, string id input
                new Object[]{SYNCHRONOUS, new StringInputIdGenerator(), strings( AUTO ), startingFromTheBeginning(), true},
                // synchronous I/O, string id input
                new Object[]{SYNCHRONOUS, new StringInputIdGenerator(), strings( AUTO ), startingFromTheBeginning(), false},
                // extra slow parallel I/O, actual node id input
                new Object[]{new IoQueue( 4, 4, 30, synchronousSlowWriterFactory ),
                        new LongInputIdGenerator(), actual(), fromInput(), false}

        );
    }

    public ParallelBatchImporterTest( WriterFactory writerFactory, InputIdGenerator inputIdGenerator,
            IdMapper idMapper, IdGenerator idGenerator, boolean multiPassIterators )
    {
        this.multiPassIterators = multiPassIterators;
        this.writerFactory = constant( writerFactory );
        this.inputIdGenerator = inputIdGenerator;
        this.idMapper = idMapper;
        this.idGenerator = idGenerator;
    }

    @Test
    public void shouldImportCsvData() throws Exception
    {
        // GIVEN
        ExecutionMonitor processorAssigner = eagerRandomSaturation( config.maxNumberOfProcessors() );
        final BatchImporter inserter = new ParallelBatchImporter( directory.absolutePath(),
                new DefaultFileSystemAbstraction(), config, new DevNullLoggingService(),
                processorAssigner, writerFactory, EMPTY );

        boolean successful = false;
        IdGroupDistribution groups = new IdGroupDistribution( NODE_COUNT, 5, random.random() );
        try
        {
            // WHEN
            inserter.doImport( Inputs.input(
                    nodes( NODE_COUNT, inputIdGenerator, groups ),
                    relationships( RELATIONSHIP_COUNT, inputIdGenerator, groups ),
                    idMapper, idGenerator, false ) );

            // THEN
            GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
            try ( Transaction tx = db.beginTx() )
            {
                inputIdGenerator.reset();
                verifyData( NODE_COUNT, RELATIONSHIP_COUNT, db, groups );
                tx.success();
            }
            finally
            {
                db.shutdown();
            }
            assertConsistent( directory.absolutePath() );
            successful = true;
        }
        finally
        {
            if ( !successful )
            {
                File failureFile = directory.file( "input" );
                try ( PrintStream out = new PrintStream( failureFile ) )
                {
                    out.println( "Seed used in this failing run: " + random.seed() );
                    out.println( inputIdGenerator );
                    for ( InputRelationship relationship : relationships( RELATIONSHIP_COUNT, inputIdGenerator, groups ) )
                    {
                        out.println( (relationship.hasSpecificId() ? relationship.specificId() + " " : "") +
                                relationship.startNode() + "-[:" + relationship.type() + "]->" + relationship.endNode() );
                    }

                    out.println();
                    out.println( "Processor assignments" );
                    out.println( processorAssigner.toString() );
                }
                System.err.println( "Additional debug information stored in " + failureFile );
            }
        }
    }

    private void assertConsistent( String storeDir ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService consistencyChecker = new ConsistencyCheckService();
        Result result = consistencyChecker.runFullConsistencyCheck( storeDir,
                new Config( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m" ) ),
                ProgressMonitorFactory.NONE,
                StringLogger.DEV_NULL );
        assertTrue( "Database contains inconsistencies, there should be a report in " + storeDir,
                result.isSuccessful() );
    }

    private static abstract class InputIdGenerator
    {
        abstract void reset();

        abstract Object nextNodeId();

        abstract Object randomExisting( Register.Long.Out nodeIndex );

        String randomType()
        {
            return "TYPE" + random.nextInt( 3 );
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static class LongInputIdGenerator extends InputIdGenerator
    {
        private volatile int id;

        @Override
        void reset()
        {
            id = 0;
        }

        @Override
        Object nextNodeId()
        {
            return (long) id++;
        }

        @Override
        Object randomExisting( Register.Long.Out nodeIndex )
        {
            long index = random.nextInt( NODE_COUNT );
            nodeIndex.write( index );
            return index;
        }
    }

    private static class StringInputIdGenerator extends InputIdGenerator
    {
        private final List<String> strings = new ArrayList<>();
        private int index;

        @Override
        void reset()
        {
            index = 0;
        }

        @Override
        Object nextNodeId()
        {
            String result;
            if ( index >= strings.size() )
            {
                strings.add( result = UUID.randomUUID().toString() );
            }
            else
            {
                result = strings.get( index );
            }
            index++;
            return result;
        }

        @Override
        Object randomExisting( Register.Long.Out nodeIndex )
        {
            int index = random.nextInt( strings.size() );
            nodeIndex.write( index );
            return strings.get( index );
        }
    }

    protected void verifyData( int nodeCount, int relationshipCount, GraphDatabaseService db, IdGroupDistribution groups )
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

        // Read all nodes, relationships and properties ad verify against the input data.
        try ( InputIterator<InputNode> nodes = nodes( nodeCount, inputIdGenerator, groups ).iterator();
              InputIterator<InputRelationship> relationships = relationships( relationshipCount, inputIdGenerator, groups ).iterator() )
        {
            // Nodes
            Map<String,Node> nodeByInputId = new HashMap<>( nodeCount );
            Iterator<Node> dbNodes = GlobalGraphOperations.at( db ).getAllNodes().iterator();
            int verifiedNodes = 0;
            while ( nodes.hasNext() )
            {
                InputNode input = nodes.next();
                Node node = dbNodes.next();
                assertNodeEquals( input, node );
                String inputId = uniqueId( input.group(), node );
                assertNull( nodeByInputId.put( inputId, node ) );
                verifiedNodes++;
            }
            assertEquals( nodeCount, verifiedNodes );

            // Relationships
            Map<String,Relationship> relationshipByName = new HashMap<>();
            for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
            {
                relationshipByName.put( (String) relationship.getProperty( "name" ), relationship );
            }
            int verifiedRelationships = 0;
            while ( relationships.hasNext() )
            {
                InputRelationship input = relationships.next();
                String name = (String) propertyOf( input, "name" );
                Relationship relationship = relationshipByName.get( name );
                assertEquals( nodeByInputId.get( uniqueId( input.startNodeGroup(), input.startNode() ) ),
                        relationship.getStartNode() );
                assertEquals( nodeByInputId.get( uniqueId( input.endNodeGroup(), input.endNode() ) ),
                        relationship.getEndNode() );
                assertRelationshipEquals( input, relationship );
                verifiedRelationships++;
            }
            assertEquals( relationshipCount, verifiedRelationships );
        }

        // The label scan store should find all nodes since they all have the same labels
        Label firstLabel = DynamicLabel.label( LABELS[0] );
        ResourceIterator<Node> allNodesWithLabel = db.findNodes( firstLabel );
        assertEquals( count( allNodesWithLabel ), nodeCount );

        // All nodes also have the same age=10 property, so we should again find them all
        ResourceIterator<Node> foundNodes = db.findNodes( firstLabel, "age", 10 );
        assertEquals( count( foundNodes ), nodeCount );
    }

    private String uniqueId( Group group, PropertyContainer entity )
    {
        return uniqueId( group, entity.getProperty( "id" ) );
    }

    private String uniqueId( Group group, Object id )
    {
        return group.name() + "_" + id;
    }

    private Object propertyOf( InputEntity input, String key )
    {
        Object[] properties = input.properties();
        for ( int i = 0; i < properties.length; i++ )
        {
            if ( properties[i++].equals( key ) )
            {
                return properties[i];
            }
        }
        throw new IllegalStateException( key + " not found on " + input );
    }

    private void assertRelationshipEquals( InputRelationship input, Relationship relationship )
    {
        // properties
        assertPropertiesEquals( input, relationship );

        // type
        assertEquals( input.type(), relationship.getType().name() );
    }

    private void assertNodeEquals( InputNode input, Node node )
    {
        // properties
        assertPropertiesEquals( input, node );

        // labels
        Set<String> expectedLabels = asSet( input.labels() );
        for ( Label label : node.getLabels() )
        {
            assertTrue( expectedLabels.remove( label.name() ) );
        }
        assertTrue( expectedLabels.isEmpty() );
    }

    private void assertPropertiesEquals( InputEntity input, PropertyContainer entity )
    {
        Object[] properties = input.properties();
        for ( int i = 0; i < properties.length; i++ )
        {
            String key = (String) properties[i++];
            Object value = properties[i];
            assertPropertyValueEquals( input, entity, key, value, entity.getProperty( key ) );
        }
    }

    private void assertPropertyValueEquals( InputEntity input, PropertyContainer entity, String key,
            Object expected, Object array )
    {
        if ( expected.getClass().isArray() )
        {
            int length = Array.getLength( expected );
            assertEquals( input + ", " + entity, length, Array.getLength( array ) );
            for ( int i = 0; i < length; i++ )
            {
                assertPropertyValueEquals( input, entity, key, Array.get( expected, i ), Array.get( array, i ) );
            }
        }
        else
        {
            assertEquals( input + ", " + entity + " for key:" + key, expected, array );
        }
    }

    private static WriterFactory synchronousSlowWriterFactory = new WriterFactories.SingleThreadedWriterFactory()
    {
        @Override
        public Writer create( final StoreChannel channel, final Monitor monitor )
        {
            return new Writer()
            {
                final Writer delegate = SYNCHRONOUS.create( channel, monitor );
                final Random random = new Random();

                @Override
                public void write( ByteBuffer data, long position, Pool<ByteBuffer> pool )
                        throws IOException
                {
                    if ( random.nextInt( 7 ) == 0 )
                    {
                        LockSupport.parkNanos( random.nextInt( 500 ) * 1_000_000 ); // slowness comes from here
                    }
                    delegate.write( data, position, pool );
                }
            };
        }

        @Override
        public void awaitEverythingWritten()
        { //noop
        }

        @Override
        public void shutdown()
        { //noop
        }

        @Override
        public String toString()
        {
            return "Randomly slow";
        }
    };

    private InputIterable<InputRelationship> relationships( final long count, final InputIdGenerator idGenerator,
            final IdGroupDistribution groups )
    {
        return new InputIterable<InputRelationship>()
        {
            private int calls;

            @Override
            public InputIterator<InputRelationship> iterator()
            {
                calls++;
                assertTrue( "Unexpected use of input iterator " + multiPassIterators + ", " + calls,
                        multiPassIterators || (!multiPassIterators && calls == 1) );

                // we still do the reset, even if tell the batch importer to not use use this iterable multiple times,
                // since we use it to compare the imported data against after the import has been completed.
                random.reset();
                return new SimpleInputIterator<InputRelationship>( "test relationships" )
                {
                    private int cursor;
                    private final Register.LongRegister nodeIndex = Registers.newLongRegister();

                    @Override
                    protected InputRelationship fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            Object[] properties = new Object[]{
                                    "name", "Nisse " + cursor,
                                    "age", 10,
                                    "long-string", "OK here goes... a long string that will certainly end up in a dynamic " +
                                    "record1234567890!@#$%^&*()_|",
                                    "array", new long[]{1234567890123L, 987654321987L, 123456789123L, 987654321987L}
                            };

                            try
                            {
                                Object startNode = idGenerator.randomExisting( nodeIndex );
                                Group startNodeGroup = groups.groupOf( nodeIndex.read() );
                                Object endNode = idGenerator.randomExisting( nodeIndex );
                                Group endNodeGroup = groups.groupOf( nodeIndex.read() );
                                return new InputRelationship(
                                        sourceDescription, itemNumber, itemNumber,
                                        properties, null,
                                        startNodeGroup, startNode, endNodeGroup, endNode,
                                        idGenerator.randomType(), null );
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

            @Override
            public boolean supportsMultiplePasses()
            {
                return multiPassIterators;
            }
        };
    }

    private InputIterable<InputNode> nodes( final long count, final InputIdGenerator inputIdGenerator,
            final IdGroupDistribution groups )
    {
        return new InputIterable<InputNode>()
        {
            private int calls;

            @Override
            public InputIterator<InputNode> iterator()
            {
                calls++;
                assertTrue( "Unexpected use of input iterator " + multiPassIterators + ", " + calls,
                        multiPassIterators || (!multiPassIterators && calls == 1) );

                return new SimpleInputIterator<InputNode>( "test nodes" )
                {
                    private int cursor;

                    @Override
                    protected InputNode fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            Object nodeId = inputIdGenerator.nextNodeId();
                            Object[] properties = new Object[]{
                                    "name", "Nisse " + cursor,
                                    "age", 10,
                                    "long-string", "OK here goes... a long string that will certainly end up in a dynamic " +
                                    "record1234567890!@#$%^&*()_|",
                                    "array", new long[]{1234567890123L, 987654321987L, 123456789123L, 987654321987L},
                                    "id", nodeId
                            };

                            try
                            {
                                Group group = groups.groupOf( cursor );
                                return new InputNode( sourceDescription, itemNumber, itemNumber, group,
                                        nodeId, properties, null, LABELS, null );
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

            @Override
            public boolean supportsMultiplePasses()
            {
                return multiPassIterators;
            }
        };
    }

    public static final @ClassRule RandomRule random = new RandomRule();
}
