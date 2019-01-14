/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

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
import org.neo4j.graphdb.ResourceIterator;
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
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.InputIterable.replayable;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.AUTO_WITHOUT_PAGECACHE;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers.longs;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers.strings;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Inputs.knownEstimates;
import static org.neo4j.unsafe.impl.batchimport.staging.ProcessorAssignmentStrategies.eagerRandomSaturation;

@RunWith( Parameterized.class )
public class ParallelBatchImporterTest
{
    private static final int NUMBER_OF_ID_GROUPS = 5;
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
    private final Function<Groups,IdMapper> idMapper;

    @Parameterized.Parameters( name = "{0},{1},{3}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList(
                // Long input ids, actual node id input
                new Object[]{new LongInputIdGenerator(), (Function<Groups,IdMapper>) groups -> longs( AUTO_WITHOUT_PAGECACHE, groups )},
                // String input ids, generate ids from stores
                new Object[]{new StringInputIdGenerator(), (Function<Groups,IdMapper>) groups -> strings( AUTO_WITHOUT_PAGECACHE, groups )}
        );
    }

    public ParallelBatchImporterTest( InputIdGenerator inputIdGenerator, Function<Groups,IdMapper> idMapper )
    {
        this.inputIdGenerator = inputIdGenerator;
        this.idMapper = idMapper;
    }

    @Test
    public void shouldImportCsvData() throws Exception
    {
        // GIVEN
        ExecutionMonitor processorAssigner = eagerRandomSaturation( config.maxNumberOfProcessors() );
        File storeDir = directory.directory( "dir" + random.string( 8, 8, Randoms.CSA_LETTERS_AND_DIGITS ) );
        storeDir.mkdirs();
        final BatchImporter inserter = new ParallelBatchImporter( storeDir,
                fileSystemRule.get(), null, config, NullLogService.getInstance(),
                processorAssigner, EMPTY, Config.defaults(), getFormat(), NO_MONITOR );

        boolean successful = false;
        Groups groups = new Groups();
        IdGroupDistribution groupDistribution = new IdGroupDistribution( NODE_COUNT, NUMBER_OF_ID_GROUPS, random.random(), groups );
        long nodeRandomSeed = random.nextLong();
        long relationshipRandomSeed = random.nextLong();
        try
        {
            // WHEN
            inserter.doImport( Inputs.input(
                    nodes( nodeRandomSeed, NODE_COUNT, config.batchSize(), inputIdGenerator, groupDistribution ),
                    relationships( relationshipRandomSeed, RELATIONSHIP_COUNT, config.batchSize(),
                            inputIdGenerator, groupDistribution ), idMapper.apply( groups ),
                    /*insanely high bad tolerance, but it will actually never be that many*/
                    silentBadCollector( RELATIONSHIP_COUNT ),
                    knownEstimates(
                            NODE_COUNT, RELATIONSHIP_COUNT,
                            NODE_COUNT * TOKENS.length / 2,
                            RELATIONSHIP_COUNT * TOKENS.length / 2,
                            NODE_COUNT * TOKENS.length / 2 * Long.BYTES,
                            RELATIONSHIP_COUNT * TOKENS.length / 2 * Long.BYTES,
                            NODE_COUNT * TOKENS.length / 2 ) ) );

            // THEN
            GraphDatabaseService db = new TestGraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( storeDir )
                    .setConfig( "dbms.backup.enabled", "false" )
                    .newGraphDatabase();
            try ( Transaction tx = db.beginTx() )
            {
                inputIdGenerator.reset();
                verifyData( NODE_COUNT, RELATIONSHIP_COUNT, db, groupDistribution, nodeRandomSeed, relationshipRandomSeed );
                tx.success();
            }
            finally
            {
                db.shutdown();
            }
            assertConsistent( storeDir );
            successful = true;
        }
        finally
        {
            if ( !successful )
            {
                File failureFile = new File( storeDir, "input" );
                try ( PrintStream out = new PrintStream( failureFile ) )
                {
                    out.println( "Seed used in this failing run: " + random.seed() );
                    out.println( inputIdGenerator );
                    inputIdGenerator.reset();
                    out.println();
                    out.println( "Processor assignments" );
                    out.println( processorAssigner.toString() );
                }
                System.err.println( "Additional debug information stored in " + failureFile );
            }
        }
    }

    protected void assertConsistent( File storeDir ) throws ConsistencyCheckIncompleteException
    {
        ConsistencyCheckService consistencyChecker = new ConsistencyCheckService();
        Result result = consistencyChecker.runFullConsistencyCheck( storeDir,
                Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" ),
                ProgressMonitorFactory.NONE,
                NullLogProvider.getInstance(), false );
        assertTrue( "Database contains inconsistencies, there should be a report in " + storeDir,
                result.isSuccessful() );
    }

    protected RecordFormats getFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }

    private static class ExistingId
    {
        private final Object id;
        private final long nodeIndex;

        ExistingId( Object id, long nodeIndex )
        {
            this.id = id;
            this.nodeIndex = nodeIndex;
        }
    }

    public abstract static class InputIdGenerator
    {
        abstract void reset();

        abstract Object nextNodeId( Random random, long item );

        abstract ExistingId randomExisting( Random random );

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
        @Override
        void reset()
        {
        }

        @Override
        synchronized Object nextNodeId( Random random, long item )
        {
            return item;
        }

        @Override
        ExistingId randomExisting( Random random )
        {
            long index = random.nextInt( NODE_COUNT );
            return new ExistingId( index, index );
        }

        @Override
        Object miss( Random random, Object id, float chance )
        {
            return random.nextFloat() < chance ? (Long) id + 100_000_000 : id;
        }

        @Override
        boolean isMiss( Object id )
        {
            return (Long) id >= 100_000_000;
        }
    }

    private static class StringInputIdGenerator extends InputIdGenerator
    {
        private final String[] strings = new String[NODE_COUNT];

        @Override
        void reset()
        {
            Arrays.fill( strings, null );
        }

        @Override
        Object nextNodeId( Random random, long item )
        {
            byte[] randomBytes = new byte[10];
            random.nextBytes( randomBytes );
            String result = UUID.nameUUIDFromBytes( randomBytes ).toString();
            strings[toIntExact( item )] = result;
            return result;
        }

        @Override
        ExistingId randomExisting( Random random )
        {
            int index = random.nextInt( strings.length );
            return new ExistingId( strings[index], index );
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
            long nodeRandomSeed, long relationshipRandomSeed ) throws IOException
    {
        // Read all nodes, relationships and properties ad verify against the input data.
        try ( InputIterator nodes = nodes( nodeRandomSeed, nodeCount, config.batchSize(), inputIdGenerator, groups ).iterator();
              InputIterator relationships = relationships( relationshipRandomSeed, relationshipCount,
                      config.batchSize(), inputIdGenerator, groups ).iterator();
              ResourceIterator<Node> dbNodes = db.getAllNodes().iterator() )
        {
            // Nodes
            Map<String,Node> nodeByInputId = new HashMap<>( nodeCount );
            while ( dbNodes.hasNext() )
            {
                Node node = dbNodes.next();
                String id = (String) node.getProperty( "id" );
                assertNull( nodeByInputId.put( id, node ) );
            }

            int verifiedNodes = 0;
            long allNodesScanLabelCount = 0;
            InputChunk chunk = nodes.newChunk();
            InputEntity input = new InputEntity();
            while ( nodes.next( chunk ) )
            {
                while ( chunk.next( input ) )
                {
                    String iid = uniqueId( input.idGroup, input.objectId );
                    Node node = nodeByInputId.get( iid );
                    assertNodeEquals( input, node );
                    verifiedNodes++;
                    assertDegrees( node );
                    allNodesScanLabelCount += Iterables.count( node.getLabels() );
                }
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
            chunk = relationships.newChunk();
            Map<String,Relationship> relationshipByName = new HashMap<>();
            for ( Relationship relationship : db.getAllRelationships() )
            {
                relationshipByName.put( (String) relationship.getProperty( "id" ), relationship );
            }
            int verifiedRelationships = 0;
            while ( relationships.next( chunk ) )
            {
                while ( chunk.next( input ) )
                {
                    if ( !inputIdGenerator.isMiss( input.objectStartId ) &&
                         !inputIdGenerator.isMiss( input.objectEndId ) )
                    {
                        // A relationship referring to missing nodes. The InputIdGenerator is expected to generate
                        // some (very few) of those. Skip it.
                        String name = (String) propertyOf( input, "id" );
                        Relationship relationship = relationshipByName.get( name );
                        assertNotNull( "Expected there to be a relationship with name '" + name + "'", relationship );
                        assertEquals( nodeByInputId.get( uniqueId( input.startIdGroup, input.objectStartId ) ),
                                relationship.getStartNode() );
                        assertEquals( nodeByInputId.get( uniqueId( input.endIdGroup, input.objectEndId ) ),
                                relationship.getEndNode() );
                        assertRelationshipEquals( input, relationship );
                    }
                    verifiedRelationships++;
                }
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

    private void assertRelationshipEquals( InputEntity input, Relationship relationship )
    {
        // properties
        assertPropertiesEquals( input, relationship );

        // type
        assertEquals( input.stringType, relationship.getType().name() );
    }

    private void assertNodeEquals( InputEntity input, Node node )
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
            assertEquals( input + ", " + entity + " for key:" + key, Values.of( expected ), Values.of( array ) );
        }
    }

    private InputIterable relationships( final long randomSeed, final long count, int batchSize,
            final InputIdGenerator idGenerator, final IdGroupDistribution groups )
    {
        return replayable( () -> new GeneratingInputIterator<>( count, batchSize, new RandomsStates( randomSeed ),
                ( randoms, visitor, id ) -> {
                    randomProperties( randoms, "Name " + id, visitor );
                    ExistingId startNodeExistingId = idGenerator.randomExisting( randoms.random() );
                    Group startNodeGroup = groups.groupOf( startNodeExistingId.nodeIndex );
                    ExistingId endNodeExistingId = idGenerator.randomExisting( randoms.random() );
                    Group endNodeGroup = groups.groupOf( endNodeExistingId.nodeIndex );

                    // miss some
                    Object startNode = idGenerator.miss( randoms.random(), startNodeExistingId.id, 0.001f );
                    Object endNode = idGenerator.miss( randoms.random(), endNodeExistingId.id, 0.001f );

                    visitor.startId( startNode, startNodeGroup );
                    visitor.endId( endNode, endNodeGroup );

                    String type = idGenerator.randomType( randoms.random() );
                    if ( randoms.random().nextFloat() < 0.00005 )
                    {
                        // Let there be a small chance of introducing a one-off relationship
                        // with a type that no, or at least very few, other relationships have.
                        type += "_odd";
                    }
                    visitor.type( type );
                }, 0 ) );
    }

    private InputIterable nodes( final long randomSeed, final long count, int batchSize,
            final InputIdGenerator inputIdGenerator, final IdGroupDistribution groups )
    {
        return replayable( () -> new GeneratingInputIterator<>( count, batchSize, new RandomsStates( randomSeed ),
                ( randoms, visitor, id ) -> {
                    Object nodeId = inputIdGenerator.nextNodeId( randoms.random(), id );
                    Group group = groups.groupOf( id );
                    visitor.id( nodeId, group );
                    randomProperties( randoms, uniqueId( group, nodeId ), visitor );
                    visitor.labels( randoms.selection( TOKENS, 0, TOKENS.length, true ) );
                }, 0 ) );
    }

    private static final String[] TOKENS = {"token1", "token2", "token3", "token4", "token5", "token6", "token7"};

    private void randomProperties( Randoms randoms, Object id, InputEntityVisitor visitor )
    {
        String[] keys = randoms.selection( TOKENS, 0, TOKENS.length, false );
        for ( String key : keys )
        {
            visitor.property( key, randoms.propertyValue() );
        }
        visitor.property( "id", id );
    }
}
