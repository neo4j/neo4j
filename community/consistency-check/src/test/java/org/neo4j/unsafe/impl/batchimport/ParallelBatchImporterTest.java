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
package org.neo4j.unsafe.impl.batchimport;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.Randoms;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.input.SimpleInputIterator;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators.fromInput;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators.startingFromTheBeginning;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers.longs;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers.strings;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.staging.ProcessorAssignmentStrategies.eagerRandomSaturation;

@RunWith( Parameterized.class )
public class ParallelBatchImporterTest
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final RandomRule random = new RandomRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( directory ).around( random ).around( fileSystemRule );

    private static final int NODE_COUNT = 10_000;
    private static final int RELATIONSHIPS_PER_NODE = 5;
    private static final int RELATIONSHIP_COUNT = NODE_COUNT * RELATIONSHIPS_PER_NODE;
    private static final int RELATIONSHIP_TYPES = 3;
    protected final Configuration config = new Configuration()
    {
        @Override
        public int batchSize()
        {
            // Set to extra low to exercise the internals a bit more.
            return 100;
        }

        @Override
        public int denseNodeThreshold()
        {
            // This will have statistically half the nodes be considered dense
            return RELATIONSHIPS_PER_NODE * 2;
        }

        @Override
        public int maxNumberOfProcessors()
        {
            // Let's really crank up the number of threads to try and flush out all and any parallelization issues.
            int cores = Runtime.getRuntime().availableProcessors();
            return random.intBetween( cores, cores + 100 );
        }

        @Override
        public long maxMemoryUsage()
        {
            // This calculation is just to try and hit some sort of memory limit so that relationship import
            // is split up into multiple rounds. Also to see that relationship group defragmentation works
            // well when doing multiple rounds.
            double ratio = NODE_COUNT / 1_000D;
            long mebi = mebiBytes( 1 );
            return random.nextInt( (int) (ratio * mebi / 2), (int) (ratio * mebi) );
        }
    };
    private final InputIdGenerator inputIdGenerator;
    private final IdMapper idMapper;
    private final IdGenerator idGenerator;
    private final boolean multiPassIterators;

    @Parameterized.Parameters(name = "{0},{1},{3}")
    public static Collection<Object[]> data()
    {
        return Arrays.<Object[]>asList(

                // synchronous I/O, actual node id input
                new Object[]{new LongInputIdGenerator(), longs( AUTO ), fromInput(), true},
                // synchronous I/O, string id input
                new Object[]{new StringInputIdGenerator(), strings( AUTO ), startingFromTheBeginning(), true},
                // synchronous I/O, string id input
                new Object[]{new StringInputIdGenerator(), strings( AUTO ), startingFromTheBeginning(), false},
                // extra slow parallel I/O, actual node id input
                new Object[]{new LongInputIdGenerator(), longs( AUTO ), fromInput(), false}
        );
    }

    public ParallelBatchImporterTest( InputIdGenerator inputIdGenerator,
            IdMapper idMapper, IdGenerator idGenerator, boolean multiPassIterators )
    {
        this.multiPassIterators = multiPassIterators;
        this.inputIdGenerator = inputIdGenerator;
        this.idMapper = idMapper;
        this.idGenerator = idGenerator;
    }

    @Test
    public void shouldImportCsvData() throws Exception
    {
        // GIVEN
        ExecutionMonitor processorAssigner = eagerRandomSaturation( config.maxNumberOfProcessors() );
        final BatchImporter inserter = new ParallelBatchImporter( directory.graphDbDir(),
                fileSystemRule.get(), config, NullLogService.getInstance(),
                processorAssigner, EMPTY, Config.empty(), getFormat() );

        boolean successful = false;
        IdGroupDistribution groups = new IdGroupDistribution( NODE_COUNT, 5, random.random() );
        long nodeRandomSeed = random.nextLong(), relationshipRandomSeed = random.nextLong();
        try
        {
            // WHEN
            inserter.doImport( Inputs.input(
                    nodes( nodeRandomSeed, NODE_COUNT, inputIdGenerator, groups ),
                    relationships( relationshipRandomSeed, RELATIONSHIP_COUNT, inputIdGenerator, groups ),
                    idMapper, idGenerator,
                    /*insanely high bad tolerance, but it will actually never be that many*/
                    silentBadCollector( RELATIONSHIP_COUNT ) ) );

            // THEN
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( directory.graphDbDir() )
                    .newGraphDatabase();
            try ( Transaction tx = db.beginTx() )
            {
                inputIdGenerator.reset();
                verifyData( NODE_COUNT, RELATIONSHIP_COUNT, db, groups, nodeRandomSeed, relationshipRandomSeed );
                tx.success();
            }
            finally
            {
                db.shutdown();
            }
            assertConsistent( directory.graphDbDir() );
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
                    inputIdGenerator.reset();
                    for ( InputNode node : nodes( nodeRandomSeed, NODE_COUNT, inputIdGenerator, groups ) )
                    {
                        out.println( node );
                    }
                    for ( InputRelationship relationship : relationships( relationshipRandomSeed,
                            RELATIONSHIP_COUNT, inputIdGenerator, groups ) )
                    {
                        out.println( relationship );
                    }

                    out.println();
                    out.println( "Processor assignments" );
                    out.println( processorAssigner.toString() );
                }
                System.err.println( "Additional debug information stored in " + failureFile );
            }
        }
    }

    private void assertConsistent( File storeDir ) throws ConsistencyCheckIncompleteException, IOException
    {
        ConsistencyCheckService consistencyChecker = new ConsistencyCheckService();
        Result result = consistencyChecker.runFullConsistencyCheck( storeDir,
                Config.embeddedDefaults( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m" ) ),
                ProgressMonitorFactory.NONE,
                NullLogProvider.getInstance(), false );
        assertTrue( "Database contains inconsistencies, there should be a report in " + storeDir,
                result.isSuccessful() );
    }

    protected RecordFormats getFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }

    public abstract static class InputIdGenerator
    {
        abstract void reset();

        abstract Object nextNodeId( Random random );

        abstract Object randomExisting( Random random, MutableLong nodeIndex );

        abstract Object miss( Random random, Object id, float chance );

        abstract boolean isMiss( Object id );

        String randomType( Random random )
        {
            return "TYPE" + random.nextInt( RELATIONSHIP_TYPES );
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
        Object nextNodeId( Random random )
        {
            return (long) id++;
        }

        @Override
        Object randomExisting( Random random, MutableLong nodeIndex )
        {
            long index = random.nextInt( NODE_COUNT );
            nodeIndex.setValue( index );
            return index;
        }

        @Override
        Object miss( Random random, Object id, float chance )
        {
            return random.nextFloat() < chance ? ((Long)id).longValue() + 100_000_000 : id;
        }

        @Override
        boolean isMiss( Object id )
        {
            return ((Long)id).longValue() >= 100_000_000;
        }
    }

    private static class StringInputIdGenerator extends InputIdGenerator
    {
        private final byte[] randomBytes = new byte[10];
        private final List<String> strings = new ArrayList<>();

        @Override
        void reset()
        {
            strings.clear();
        }

        @Override
        Object nextNodeId( Random random )
        {
            random.nextBytes( randomBytes );
            String result = UUID.nameUUIDFromBytes( randomBytes ).toString();
            strings.add( result );
            return result;
        }

        @Override
        Object randomExisting( Random random, MutableLong nodeIndex )
        {
            int index = random.nextInt( strings.size() );
            nodeIndex.setValue( index );
            return strings.get( index );
        }

        @Override
        Object miss( Random random, Object id, float chance )
        {
            return random.nextFloat() < chance ? "_" + id : id;
        }

        @Override
        boolean isMiss( Object id )
        {
            return ((String)id).startsWith( "_" );
        }
    }

    private void verifyData( int nodeCount, int relationshipCount, GraphDatabaseService db, IdGroupDistribution groups,
            long nodeRandomSeed, long relationshipRandomSeed )
    {
        // Read all nodes, relationships and properties ad verify against the input data.
        try ( InputIterator<InputNode> nodes = nodes( nodeRandomSeed, nodeCount, inputIdGenerator, groups ).iterator();
              InputIterator<InputRelationship> relationships = relationships( relationshipRandomSeed, relationshipCount,
                      inputIdGenerator, groups ).iterator() )
        {
            // Nodes
            Map<String,Node> nodeByInputId = new HashMap<>( nodeCount );
            Iterator<Node> dbNodes = db.getAllNodes().iterator();
            int verifiedNodes = 0;
            long allNodesScanLabelCount = 0;
            while ( nodes.hasNext() )
            {
                InputNode input = nodes.next();
                Node node = dbNodes.next();
                assertNodeEquals( input, node );
                String inputId = uniqueId( input.group(), node );
                assertNull( nodeByInputId.put( inputId, node ) );
                verifiedNodes++;
                assertDegrees( node );
                allNodesScanLabelCount += Iterables.count( node.getLabels() );
            }
            assertEquals( nodeCount, verifiedNodes );

            // Labels
            long labelScanStoreEntryCount = db.getAllLabels().stream()
                    .flatMap( l -> db.findNodes( l ).stream() )
                    .count();

            assertEquals( format( "Expected label scan store and node store to have same number labels. But %n" +
                            "#labelsInNodeStore=%d%n" +
                            "#labelsInLabelScanStore=%d%n", allNodesScanLabelCount, labelScanStoreEntryCount ),
                    allNodesScanLabelCount, labelScanStoreEntryCount );

            // Relationships
            Map<String,Relationship> relationshipByName = new HashMap<>();
            for ( Relationship relationship : db.getAllRelationships() )
            {
                relationshipByName.put( (String) relationship.getProperty( "id" ), relationship );
            }
            int verifiedRelationships = 0;
            while ( relationships.hasNext() )
            {
                InputRelationship input = relationships.next();
                if ( !inputIdGenerator.isMiss( input.startNode() ) &&
                     !inputIdGenerator.isMiss( input.endNode() ) )
                {
                    // A relationship referring to missing nodes. The InputIdGenerator is expected to generate
                    // some (very few) of those. Skip it.
                    String name = (String) propertyOf( input, "id" );
                    Relationship relationship = relationshipByName.get( name );
                    assertNotNull( "Expected there to be a relationship with name '" + name + "'", relationship );
                    assertEquals( nodeByInputId.get( uniqueId( input.startNodeGroup(), input.startNode() ) ),
                            relationship.getStartNode() );
                    assertEquals( nodeByInputId.get( uniqueId( input.endNodeGroup(), input.endNode() ) ),
                            relationship.getEndNode() );
                    assertRelationshipEquals( input, relationship );
                }
                verifiedRelationships++;
            }
            assertEquals( relationshipCount, verifiedRelationships );
        }
    }

    private void assertDegrees( Node node )
    {
        for ( RelationshipType type : node.getRelationshipTypes() )
        {
            for ( Direction direction : Direction.values() )
            {
                long degree = node.getDegree( type, direction );
                long actualDegree = count( node.getRelationships( type, direction ) );
                assertEquals( actualDegree, degree );
            }
        }
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

    private InputIterable<InputRelationship> relationships( final long randomSeed, final long count,
            final InputIdGenerator idGenerator, final IdGroupDistribution groups )
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
                return new SimpleInputIterator<InputRelationship>( "test relationships" )
                {
                    private final Random random = new Random( randomSeed );
                    private final Randoms randoms = new Randoms( random, Randoms.DEFAULT );
                    private int cursor;
                    private final MutableLong nodeIndex = new MutableLong( -1 );

                    @Override
                    protected InputRelationship fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            Object[] properties = randomProperties( randoms, "Name " + cursor );
                            try
                            {
                                Object startNode = idGenerator.randomExisting( random, nodeIndex );
                                Group startNodeGroup = groups.groupOf( nodeIndex.longValue() );
                                Object endNode = idGenerator.randomExisting( random, nodeIndex );
                                Group endNodeGroup = groups.groupOf( nodeIndex.longValue() );

                                // miss some
                                startNode = idGenerator.miss( random, startNode, 0.001f );
                                endNode = idGenerator.miss( random, endNode, 0.001f );

                                String type = idGenerator.randomType( random );
                                if ( random.nextFloat() < 0.00005 )
                                {
                                    // Let there be a small chance of introducing a one-off relationship
                                    // with a type that no, or at least very few, other relationships have.
                                    type += "_odd";
                                }
                                return new InputRelationship(
                                        sourceDescription, itemNumber, itemNumber,
                                        properties, null,
                                        startNodeGroup, startNode, endNodeGroup, endNode,
                                        type, null );
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

    private InputIterable<InputNode> nodes( final long randomSeed, final long count,
            final InputIdGenerator inputIdGenerator, final IdGroupDistribution groups )
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
                    private final Random random = new Random( randomSeed );
                    private final Randoms randoms = new Randoms( random, Randoms.DEFAULT );
                    private int cursor;

                    @Override
                    protected InputNode fetchNextOrNull()
                    {
                        if ( cursor < count )
                        {
                            Object nodeId = inputIdGenerator.nextNodeId( random );
                            Object[] properties = randomProperties( randoms, nodeId );
                            String[] labels = randoms.selection( TOKENS, 0, TOKENS.length, true );
                            try
                            {
                                Group group = groups.groupOf( cursor );
                                return new InputNode( sourceDescription, itemNumber, itemNumber, group,
                                        nodeId, properties, null, labels, null );
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

    private static final String[] TOKENS = {"token1", "token2", "token3", "token4", "token5", "token6", "token7"};

    private Object[] randomProperties( Randoms randoms, Object id )
    {
        String[] keys = randoms.selection( TOKENS, 0, TOKENS.length, false );
        Object[] properties = new Object[(keys.length + 1) * 2];
        for ( int i = 0; i < keys.length; i++ )
        {
            properties[i * 2] = keys[i];
            properties[i * 2 + 1] = randoms.propertyValue();
        }
        properties[properties.length - 2] = "id";
        properties[properties.length - 1] = id;
        return properties;
    }
}
