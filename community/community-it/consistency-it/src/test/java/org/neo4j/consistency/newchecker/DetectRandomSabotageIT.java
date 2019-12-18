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
package org.neo4j.consistency.newchecker;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.LabelScanWriter;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.AbstractDelegatingIndexProxy;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexProxy;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.experimental_consistency_checker;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

@ExtendWith( {TestDirectorySupportExtension.class, RandomExtension.class} )
public class DetectRandomSabotageIT
{
    private static final int SOME_WAY_TOO_HIGH_ID = 10_000_000;
    private static final int NUMBER_OF_NODES = 10_000;
    private static final int NUMBER_OF_INDEXES = 7;
    private static final String[] TOKEN_NAMES = {"One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight"};

    @Inject
    TestDirectory directory;

    @Inject
    protected RandomRule random;

    private DatabaseManagementService dbms;
    private NeoStores neoStores;
    private DependencyResolver resolver;

    protected DatabaseManagementService getDbms( File home )
    {
        return new TestDatabaseManagementServiceBuilder( home ).build();
    }

    @BeforeEach
    void setUp()
    {
        dbms = getDbms( directory.homeDir() );
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );

        // Create some nodes
        MutableLongList nodeIds = createNodes( db );

        // Force some nodes to be dense nodes and some to have only a single relationship
        MutableLongSet singleRelationshipNodes = LongSets.mutable.empty();
        MutableLongSet denseNodes = LongSets.mutable.empty();
        while ( singleRelationshipNodes.size() < 5 )
        {
            singleRelationshipNodes.add( nodeIds.get( random.nextInt( nodeIds.size() ) ) );
        }
        while ( denseNodes.size() < 5 )
        {
            long nodeId = nodeIds.get( random.nextInt( nodeIds.size() ) );
            if ( !singleRelationshipNodes.contains( nodeId ) )
            {
                denseNodes.add( nodeId );
            }
        }

        // Connect them with some relationships
        createRelationships( db, nodeIds, singleRelationshipNodes );

        // Make the dense nodes dense by creating many relationships to or from them
        createAdditionalRelationshipsForDenseNodes( db, nodeIds, denseNodes );

        // Delete some entities
        deleteSomeEntities( db, nodeIds, singleRelationshipNodes, denseNodes );

        // Create some indexes and constraints
        createSchema( db );

        neoStores = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        resolver = db.getDependencyResolver();
    }

    @AfterEach
    void tearDown()
    {
        dbms.shutdown();
    }

    @Test
    void shouldDetectRandomSabotage() throws Exception
    {
        // given
        SabotageType type = random.among( SabotageType.values() );

        // when
        Sabotage sabotage = type.run( random, neoStores, resolver );

        // then
        ConsistencyCheckService.Result result = shutDownAndRunConsistencyChecker();
        boolean hasSomeErrorOrWarning = result.summary().getTotalInconsistencyCount() > 0 || result.summary().getTotalWarningCount() > 0;
        assertTrue( hasSomeErrorOrWarning );
        // TODO also assert there being a report about the sabotaged area
    }

    private void createSchema( GraphDatabaseAPI db )
    {
        for ( int i = 0; i < NUMBER_OF_INDEXES; i++ )
        {
            Label label = Label.label( random.among( TOKEN_NAMES ) );
            String[] keys = random.selection( TOKEN_NAMES, 1, 3, false );
            try ( Transaction tx = db.beginTx() )
            {
                // A couple of node indexes
                IndexCreator indexCreator = tx.schema().indexFor( label );
                for ( String key : keys )
                {
                    indexCreator = indexCreator.on( key );
                }
                indexCreator.create();
                tx.commit();
            }
            catch ( ConstraintViolationException e )
            {
                // This is fine we've already created a similar index
            }

            if ( keys.length == 1 && random.nextFloat() < 0.3 )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    // Also create a uniqueness constraint for this index
                    ConstraintCreator constraintCreator = tx.schema().constraintFor( label );
                    for ( String key : keys )
                    {
                        constraintCreator = constraintCreator.assertPropertyIsUnique( key );
                    }
                    // TODO also make it so that it's possible to add other types of constraints... this would mean
                    //      guaranteeing e.g. property existence on entities given certain entity tokens and such
                    constraintCreator.create();
                    tx.commit();
                }
                catch ( ConstraintViolationException e )
                {
                    // This is fine, either we tried to create a uniqueness constraint on data that isn't unique (we just generate random data above)
                    // or we already created a similar constraint.
                }
            }
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private void deleteSomeEntities( GraphDatabaseAPI db, MutableLongList nodeIds, MutableLongSet singleRelationshipNodes, MutableLongSet denseNodes )
    {
        int nodesToDelete = NUMBER_OF_NODES / 100;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodesToDelete; i++ )
            {
                long nodeId;
                do
                {
                    nodeId = nodeIds.get( random.nextInt( nodeIds.size() ) );
                }
                while ( singleRelationshipNodes.contains( nodeId ) || denseNodes.contains( nodeId ) );
                nodeIds.remove( nodeId );
                Node node = tx.getNodeById( nodeId );
                node.getRelationships().forEach( Relationship::delete );
                node.delete();
            }
            tx.commit();
        }
    }

    private void createAdditionalRelationshipsForDenseNodes( GraphDatabaseAPI db, MutableLongList nodeIds, MutableLongSet denseNodes )
    {
        try ( Transaction tx = db.beginTx() )
        {
            int additionalRelationships = denseNodes.size() * GraphDatabaseSettings.dense_node_threshold.defaultValue();
            long[] denseNodeIds = denseNodes.toArray();
            for ( int i = 0; i < additionalRelationships; i++ )
            {
                Node denseNode = tx.getNodeById( denseNodeIds[i % denseNodeIds.length] );
                Node otherNode = tx.getNodeById( nodeIds.get( random.nextInt( nodeIds.size() ) ) );
                Node startNode = random.nextBoolean() ? denseNode : otherNode;
                Node endNode = startNode == denseNode ? otherNode : denseNode;
                startNode.createRelationshipTo( endNode, RelationshipType.withName( random.among( TOKEN_NAMES ) ) );
            }
            tx.commit();
        }
    }

    private void createRelationships( GraphDatabaseAPI db, MutableLongList nodeIds, MutableLongSet singleRelationshipNodes )
    {
        try ( Transaction tx = db.beginTx() )
        {
            int numberOfRelationships = (int) (NUMBER_OF_NODES * (10f + 10f * random.nextFloat()));
            for ( int i = 0; i < numberOfRelationships; i++ )
            {
                Node startNode = tx.getNodeById( nodeIds.get( random.nextInt( nodeIds.size() ) ) );
                Node endNode = tx.getNodeById( nodeIds.get( random.nextInt( nodeIds.size() ) ) );
                startNode.createRelationshipTo( endNode, RelationshipType.withName( random.among( TOKEN_NAMES ) ) );
                // Prevent more relationships to be added to the "single-relationship" Nodes
                if ( singleRelationshipNodes.remove( startNode.getId() ) )
                {
                    nodeIds.remove( startNode.getId() );
                }
                if ( singleRelationshipNodes.remove( endNode.getId() ) )
                {
                    nodeIds.remove( endNode.getId() );
                }
            }
            tx.commit();
        }
    }

    private MutableLongList createNodes( GraphDatabaseAPI db )
    {
        MutableLongList nodeIds = LongLists.mutable.empty();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < NUMBER_OF_NODES; i++ )
            {
                Node node = tx.createNode( labels( random.selection( TOKEN_NAMES, 0, TOKEN_NAMES.length, false ) ) );
                setRandomProperties( node );
                nodeIds.add( node.getId() );
            }
            tx.commit();
        }
        return nodeIds;
    }

    private void setRandomProperties( Entity entity )
    {
        for ( String key : random.selection( TOKEN_NAMES, 0, TOKEN_NAMES.length, false ) )
        {
            entity.setProperty( key, randomValue().asObjectCopy() );
        }
    }

    private Value randomValue()
    {
        switch ( random.nextInt( 100 ) )
        {
        case 0: // A large string
            return random.nextAlphaNumericTextValue( 300, 500 );
        case 1: // A large string array
            int arrayLength = random.nextInt( 20, 40 );
            String[] array = new String[arrayLength];
            for ( int i = 0; i < arrayLength; i++ )
            {
                array[i] = random.nextAlphaNumericTextValue( 10, 20 ).stringValue();
            }
            return Values.stringArray( array );
        default:
            return random.nextValue();
        }
    }

    private static Label[] labels( String[] names )
    {
        return Stream.of( names ).map( Label::label ).toArray( Label[]::new );
    }

    private ConsistencyCheckService.Result shutDownAndRunConsistencyChecker() throws ConsistencyCheckIncompleteException
    {
        dbms.shutdown();
        Config config = Config.newBuilder()
                .set( neo4j_home, directory.homeDir().toPath() )
                .set( experimental_consistency_checker, true )
                .build();
        return new ConsistencyCheckService().runFullConsistencyCheck( DatabaseLayout.of( config ), config, ProgressMonitorFactory.NONE,
                NullLogProvider.getInstance(), false, ConsistencyFlags.DEFAULT );
    }

    private enum SabotageType
    {
        NODE_PROP
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdate( random, stores.getNodeStore(), usedRecord(), PrimitiveRecord::getNextProp, PrimitiveRecord::setNextProp,
                                () -> randomLargeSometimesNegative( random ) );
                    }
                },
        NODE_REL
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdate( random, stores.getNodeStore(), usedRecord(), NodeRecord::getNextRel, NodeRecord::setNextRel );
                    }
                },
        NODE_LABELS
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        NodeStore store = stores.getNodeStore();
                        NodeRecord node = randomRecord( random, store, usedRecord() );
                        NodeRecord before = store.getRecord( node.getId(), store.newRecord(), RecordLoad.NORMAL );
                        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );
                        long[] existing = nodeLabels.get( store );
                        if ( random.nextBoolean() )
                        {
                            // Change inlined
                            do
                            {
                                long labelField = random.nextLong( 0xFF_FFFFFFFFL );
                                if ( !NodeLabelsField.fieldPointsToDynamicRecordOfLabels( labelField ) )
                                {
                                    node.setLabelField( labelField, node.getDynamicLabelRecords() );
                                }
                            }
                            while ( Arrays.equals( existing, NodeLabelsField.get( node, store ) ) );
                        }
                        else
                        {
                            long existingLabelField = node.getLabelField();
                            do
                            {
                                node.setLabelField( DynamicNodeLabels.dynamicPointer( randomLargeSometimesNegative( random ) ),
                                        node.getDynamicLabelRecords() );
                            }
                            while ( existingLabelField == node.getLabelField() );
                        }
                        store.updateRecord( node );
                        return recordSabotage( before, node );
                    }
                },
        NODE_IN_USE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return setRandomRecordNotInUse( random, stores.getNodeStore() );
                    }
                },
        RELATIONSHIP_CHAIN
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        RelationshipStore store = stores.getRelationshipStore();
                        RelationshipRecord relationship = randomRecord( random, store, usedRecord() );
                        RelationshipRecord before = store.getRecord( relationship.getId(), store.newRecord(), RecordLoad.NORMAL );
                        LongSupplier rng = () -> randomIdOrSometimesDefault( random, NULL_REFERENCE.longValue() );
                        switch ( random.nextInt( 4 ) )
                        {
                        case 0: // start node prev
                            // Our consistency checker(s) doesn't verify node degrees
                            if ( !relationship.isFirstInFirstChain() )
                            {
                                guaranteedChangedId( relationship::getFirstPrevRel, relationship::setFirstPrevRel, rng );
                                break;
                            }
                        case 1: // start node next
                            guaranteedChangedId( relationship::getFirstNextRel, relationship::setFirstNextRel, rng );
                            break;
                        case 2: // end node prev
                            // Our consistency checker(s) doesn't verify node degrees
                            if ( !relationship.isFirstInSecondChain() )
                            {
                                guaranteedChangedId( relationship::getSecondPrevRel, relationship::setSecondPrevRel, rng );
                                break;
                            }
                        default: // end node next
                            guaranteedChangedId( relationship::getSecondNextRel, relationship::setSecondNextRel, rng );
                            break;
                        }
                        store.updateRecord( relationship );
                        return recordSabotage( before, relationship );
                    }
                },
        RELATIONSHIP_NODES
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        boolean startNode = random.nextBoolean();
                        ToLongFunction<RelationshipRecord> getter = startNode ? RelationshipRecord::getFirstNode : RelationshipRecord::getSecondNode;
                        BiConsumer<RelationshipRecord,Long> setter = startNode ? RelationshipRecord::setFirstNode : RelationshipRecord::setSecondNode;
                        return loadChangeUpdate( random, stores.getRelationshipStore(), usedRecord(), getter, setter );
                    }
                },
        RELATIONSHIP_PROP
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdate( random, stores.getRelationshipStore(), usedRecord(), PrimitiveRecord::getNextProp,
                                PrimitiveRecord::setNextProp );
                    }
                },
        RELATIONSHIP_TYPE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdate( random, stores.getRelationshipStore(), usedRecord(), RelationshipRecord::getType,
                                ( relationship, type ) -> relationship.setType( type.intValue() ),
                                () -> random.nextInt( TOKEN_NAMES.length * 2 ) - 1 );
                    }
                },
        RELATIONSHIP_IN_USE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return setRandomRecordNotInUse( random, stores.getRelationshipStore() );
                    }
                },
        PROPERTY_CHAIN
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        boolean prev = random.nextBoolean();
                        if ( prev )
                        {
                            return loadChangeUpdate( random, stores.getPropertyStore(), usedRecord(), PropertyRecord::getPrevProp,
                                    PropertyRecord::setPrevProp );
                        }
                        return loadChangeUpdate( random, stores.getPropertyStore(), usedRecord(), PropertyRecord::getNextProp, PropertyRecord::setNextProp,
                                () -> randomLargeSometimesNegative( random ) ); //can not detect chains split with next = -1
                    }
                },
//        PROPERTY_VALUE,
//        PROPERTY_KEY,
        PROPERTY_IN_USE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return setRandomRecordNotInUse( random, stores.getPropertyStore() );
                    }
                },
        // STRING_CHAIN - format doesn't allow us to detect these
        STRING_LENGTH
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdateDynamicChain( random, stores.getPropertyStore(), stores.getPropertyStore().getStringStore(),
                                PropertyType.STRING, record ->
                                        record.setData( Arrays.copyOf( record.getData(), random.nextInt( record.getLength() ) ) ), v -> true );
                    }
                },
//        STRING_DATA - format doesn't allow us to detect these
        STRING_IN_USE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return setRandomRecordNotInUse( random, stores.getPropertyStore().getStringStore() );
                    }
                },
        ARRAY_CHAIN
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdateDynamicChain( random, stores.getPropertyStore(), stores.getPropertyStore().getArrayStore(),
                                PropertyType.ARRAY, record ->
                                        record.setData( Arrays.copyOf( record.getData(), random.nextInt( record.getLength() ) ) ),
                                v -> v.asObjectCopy() instanceof String[] );
                    }
                },
        ARRAY_LENGTH
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdateDynamicChain( random, stores.getPropertyStore(), stores.getPropertyStore().getArrayStore(),
                                PropertyType.ARRAY, record ->
                                        record.setData( Arrays.copyOf( record.getData(), random.nextInt( record.getLength() ) ) ), v -> true );
                    }
                },
//        ARRAY_DATA?
        ARRAY_IN_USE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return setRandomRecordNotInUse( random, stores.getPropertyStore().getArrayStore() );
                    }
                },
        RELATIONSHIP_GROUP_CHAIN
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        // prev isn't stored in the record format
                        return loadChangeUpdate( random, stores.getRelationshipGroupStore(), usedRecord(), RelationshipGroupRecord::getNext,
                                RelationshipGroupRecord::setNext, () -> randomLargeSometimesNegative( random ) );
                    }
                },
        RELATIONSHIP_GROUP_TYPE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return loadChangeUpdate( random, stores.getRelationshipGroupStore(), usedRecord(), RelationshipGroupRecord::getType,
                                ( group, type ) -> group.setType( type.intValue() ),
                                () -> random.nextInt( TOKEN_NAMES.length * 2 ) - 1 );
                    }
                },
        RELATIONSHIP_GROUP_FIRST_RELATIONSHIP
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        ToLongFunction<RelationshipGroupRecord> getter;
                        BiConsumer<RelationshipGroupRecord,Long> setter;
                        switch ( random.nextInt( 3 ) )
                        {
                        case 0:
                            getter = RelationshipGroupRecord::getFirstOut;
                            setter = RelationshipGroupRecord::setFirstOut;
                            break;
                        case 1:
                            getter = RelationshipGroupRecord::getFirstIn;
                            setter = RelationshipGroupRecord::setFirstIn;
                            break;
                        default:
                            getter = RelationshipGroupRecord::getFirstLoop;
                            setter = RelationshipGroupRecord::setFirstLoop;
                            break;
                        }
                        return loadChangeUpdate( random, stores.getRelationshipGroupStore(), usedRecord(), getter, setter,
                                () -> randomLargeSometimesNegative( random ) );
                    }
                },
        RELATIONSHIP_GROUP_IN_USE
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies )
                    {
                        return setRandomRecordNotInUse( random, stores.getRelationshipGroupStore() );
                    }
                },
        SCHEMA_INDEX_ENTRY
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies ) throws Exception
                    {
                        IndexingService indexing = otherDependencies.resolveDependency( IndexingService.class );
                        boolean add = random.nextBoolean();
                        long[] indexIds = indexing.getIndexIds().toArray();
                        long indexId = indexIds[random.nextInt( indexIds.length )];
                        IndexProxy indexProxy = indexing.getIndexProxy( indexId );
                        while ( indexProxy instanceof AbstractDelegatingIndexProxy )
                        {
                            indexProxy = ((AbstractDelegatingIndexProxy) indexProxy).getDelegate();
                        }
                        IndexAccessor accessor = ((OnlineIndexProxy) indexProxy).accessor();
                        long selectedEntityId = -1;
                        Value[] selectedValues = null;
                        try ( IndexEntriesReader reader = accessor.newAllIndexEntriesReader( 1 )[0] )
                        {
                            long entityId = -1;
                            Value[] values = null;
                            while ( reader.hasNext() )
                            {
                                entityId = reader.next();
                                values = reader.values();
                                if ( random.nextFloat() < 0.01 )
                                {
                                    selectedEntityId = entityId;
                                    selectedValues = values;
                                }
                            }
                            if ( selectedValues == null && entityId != -1 )
                            {
                                selectedEntityId = entityId;
                                selectedValues = values;
                            }
                        }
                        if ( selectedEntityId == -1 )
                        {
                            throw new UnsupportedOperationException( "Something is wrong with the test, could not find index entry to sabotage" );
                        }

                        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE_IDEMPOTENT ) )
                        {
                            if ( add )
                            {
                                selectedEntityId = random.nextLong( SOME_WAY_TOO_HIGH_ID );
                                updater.process( IndexEntryUpdate.add( selectedEntityId, indexProxy.getDescriptor(), selectedValues ) );
                            }
                            else
                            {
                                updater.process( IndexEntryUpdate.remove( selectedEntityId, indexProxy.getDescriptor(), selectedValues ) );
                            }
                        }

                        return new Sabotage( String.format( "%s entityId:%d values:%s index:%s", add ? "Add" : "Remove", selectedEntityId,
                                Arrays.toString( selectedValues ), indexProxy.getDescriptor().toString() ),
                                indexProxy.getDescriptor().toString() ); // TODO more specific
                    }
                },
        LABEL_INDEX_ENTRY
                {
                    @Override
                    Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies ) throws Exception
                    {
                        LabelScanStore labelIndex = otherDependencies.resolveDependency( LabelScanStore.class );
                        boolean add = random.nextBoolean();
                        NodeStore store = stores.getNodeStore();
                        NodeRecord nodeRecord;
                        do
                        {
                            // If we're removing a label from the label index, make sure that the selected node has a label
                            nodeRecord = store.getRecord( random.nextLong( store.getHighId() ), store.newRecord(), RecordLoad.CHECK );
                        }
                        while ( !add && (!nodeRecord.inUse() || nodeRecord.getLabelField() == NO_LABELS_FIELD.longValue()) );
                        TokenHolders tokenHolders = otherDependencies.resolveDependency( TokenHolders.class );
                        Set<String> labelNames = new HashSet<>( Arrays.asList( TOKEN_NAMES ) );
                        int labelId;
                        try ( LabelScanWriter writer = labelIndex.newWriter() )
                        {
                            if ( nodeRecord.inUse() )
                            {
                                // Our node is in use, make sure it's a label it doesn't already have
                                NodeLabels labelsField = NodeLabelsField.parseLabelsField( nodeRecord );
                                long[] labelsBefore = labelsField.get( store );
                                for ( long labelIdBefore : labelsBefore )
                                {
                                    labelNames.remove( tokenHolders.labelTokens().getTokenById( (int) labelIdBefore ).name() );
                                }
                                if ( add )
                                {
                                    // Add a label to an existing node (in the label index only)
                                    labelId = labelNames.isEmpty()
                                                  ? 9999
                                                  : tokenHolders.labelTokens().getIdByName( random.among( new ArrayList<>( labelNames ) ) );
                                    long[] labelsAfter = Arrays.copyOf( labelsBefore, labelsBefore.length + 1 );
                                    labelsAfter[labelsBefore.length] = labelId;
                                    Arrays.sort( labelsAfter );
                                    writer.write( NodeLabelUpdate.labelChanges( nodeRecord.getId(), labelsBefore, labelsAfter ) );
                                }
                                else
                                {
                                    // Remove a label from an existing node (in the label index only)
                                    MutableLongList labels = LongLists.mutable.of( Arrays.copyOf( labelsBefore, labelsBefore.length ) );
                                    labelId = (int) labels.removeAtIndex( random.nextInt( labels.size() ) );
                                    long[] labelsAfter = labels.toSortedArray(); // With one of the labels removed
                                    writer.write( NodeLabelUpdate.labelChanges( nodeRecord.getId(), labelsBefore, labelsAfter ) );
                                }
                            }
                            else // Getting here means the we're adding something (see above when selecting the node)
                            {
                                // Add a label to a non-existent node (in the label index only)
                                labelId = tokenHolders.labelTokens().getIdByName( random.among( TOKEN_NAMES ) );
                                writer.write( NodeLabelUpdate.labelChanges( nodeRecord.getId(), EMPTY_LONG_ARRAY, new long[]{labelId} ) );
                            }
                        }
                        return new Sabotage( String.format( "%s labelId:%d node:%s", add ? "Add" : "Remove", labelId, nodeRecord ), nodeRecord.toString() );
                    }
                };

        protected <T extends AbstractBaseRecord> Sabotage setRandomRecordNotInUse( RandomRule random, RecordStore<T> store )
        {
            T before = randomRecord( random, store, usedRecord() );
            T record = store.getRecord( before.getId(), store.newRecord(), RecordLoad.NORMAL );
            record.setInUse( false );
            store.updateRecord( record );
            return recordSabotage( before, record );
        }

        private static <T extends AbstractBaseRecord> Predicate<T> usedRecord()
        {
            return AbstractBaseRecord::inUse;
        }

        protected <T extends AbstractBaseRecord> Sabotage loadChangeUpdate( RandomRule random, RecordStore<T> store, Predicate<T> filter,
                ToLongFunction<T> idGetter, BiConsumer<T,Long> idSetter )
        {
            return loadChangeUpdate( random, store, filter, idGetter, idSetter, () -> randomIdOrSometimesDefault( random, NULL_REFERENCE.longValue() ) );
        }

        protected <T extends AbstractBaseRecord> Sabotage loadChangeUpdate( RandomRule random, RecordStore<T> store, Predicate<T> filter,
                ToLongFunction<T> idGetter, BiConsumer<T,Long> idSetter, LongSupplier rng )
        {
            T before = randomRecord( random, store, filter );
            T record = store.getRecord( before.getId(), store.newRecord(), RecordLoad.NORMAL );
            guaranteedChangedId( () -> idGetter.applyAsLong( record ), changedId -> idSetter.accept( record, changedId ), rng );
            store.updateRecord( record );
            return recordSabotage( before, record );
        }

        private static <T extends AbstractBaseRecord> Sabotage recordSabotage( T before, T after )
        {
            return new Sabotage( String.format( "%s --> %s", before, after ), after.toString() );
        }

        protected void guaranteedChangedId( LongSupplier getter, LongConsumer setter, LongSupplier rng )
        {
            long nextProp = getter.getAsLong();
            while ( getter.getAsLong() == nextProp )
            {
                setter.accept( rng.getAsLong() );
            }
        }

        private static Sabotage loadChangeUpdateDynamicChain( RandomRule random, PropertyStore propertyStore, AbstractDynamicStore dynamicStore,
                PropertyType valueType, Consumer<DynamicRecord> vandal, Predicate<Value> checkability )
        {
            PropertyRecord propertyRecord = propertyStore.newRecord();
            while ( true )
            {
                propertyStore.getRecord( random.nextLong( propertyStore.getHighId() ), propertyRecord, RecordLoad.CHECK );
                if ( propertyRecord.inUse() )
                {
                    for ( PropertyBlock block : propertyRecord )
                    {
                        if ( block.getType() == valueType && checkability.test( block.getType().value( block, propertyStore ) ) )
                        {
                            propertyStore.ensureHeavy( block );
                            if ( block.getValueRecords().size() > 1 )
                            {
                                DynamicRecord dynamicRecord = block.getValueRecords().get( random.nextInt( block.getValueRecords().size() - 1 ) );
                                DynamicRecord before = dynamicStore.getRecord( dynamicRecord.getId(), dynamicStore.newRecord(), RecordLoad.NORMAL );
                                vandal.accept( dynamicRecord );
                                dynamicStore.updateRecord( dynamicRecord );
                                return recordSabotage( before, dynamicRecord );
                            }
                        }
                    }
                }
            }
        }

        protected long randomIdOrSometimesDefault( RandomRule random, long defaultValue )
        {
            return random.nextFloat() < 0.1 ? defaultValue : randomLargeSometimesNegative( random );
        }

        protected long randomLargeSometimesNegative( RandomRule random )
        {
            long value = random.nextLong( SOME_WAY_TOO_HIGH_ID );
            return random.nextFloat() < 0.2 ? -value : value;
        }

        protected <T extends AbstractBaseRecord> T randomRecord( RandomRule random, RecordStore<T> store, Predicate<T> filter )
        {
            long highId = store.getHighId();
            T record = store.newRecord();
            do
            {
                store.getRecord( random.nextLong( highId ), record, RecordLoad.CHECK );
            }
            while ( !filter.test( record ) );
            return record;
        }

        abstract Sabotage run( RandomRule random, NeoStores stores, DependencyResolver otherDependencies ) throws Exception;
    }

    private static class Sabotage
    {
        private final String description;
        private final String record;

        Sabotage( String description, String record )
        {
            this.description = description;
            this.record = record;
        }

        /**
         * For humans
         */
        String description()
        {
            return description;
        }

        /**
         * For grepping on in the inconsistency report
         */
        String record()
        {
            return record;
        }
    }
}
