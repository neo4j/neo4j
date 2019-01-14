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
package org.neo4j.kernel.impl.transaction.state;

import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Iterator;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingServiceFactory;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.Loaders;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.PropertyCreator;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.PropertyTraverser;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.unsafe.batchinsert.internal.DirectRecordAccess;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.empty;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.multiToken;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.storageengine.api.EntityType.NODE;
import static org.neo4j.storageengine.api.EntityType.RELATIONSHIP;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;

public class OnlineIndexUpdatesTest
{
    private static final int ENTITY_TOKEN = 1;
    private static final int OTHER_ENTITY_TOKEN = 2;
    private static final int[] ENTITY_TOKENS = {ENTITY_TOKEN};

    @Rule
    public PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();
    private NodeStore nodeStore;
    private RelationshipStore relationshipStore;
    private IndexingService indexingService;
    private PropertyPhysicalToLogicalConverter propertyPhysicalToLogicalConverter;
    private NeoStores neoStores;
    private LifeSupport life;
    private PropertyCreator propertyCreator;
    private DirectRecordAccess<PropertyRecord,PrimitiveRecord> recordAccess;

    @Before
    public void setUp() throws Exception
    {
        life = new LifeSupport();
        PageCache pageCache = storage.pageCache();
        DatabaseLayout databaseLayout = storage.directory().databaseLayout();
        Config config = Config.defaults( GraphDatabaseSettings.default_schema_provider, EMPTY.getProviderDescriptor().name() );
        NullLogProvider nullLogProvider = NullLogProvider.getInstance();
        StoreFactory storeFactory = new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( storage.fileSystem() ), pageCache,
                        storage.fileSystem(), nullLogProvider, EmptyVersionContextSupplier.EMPTY );

        neoStores = storeFactory.openAllNeoStores( true );
        neoStores.getCounts().start();
        CountsComputer.recomputeCounts( neoStores, pageCache, databaseLayout );
        nodeStore = neoStores.getNodeStore();
        relationshipStore = neoStores.getRelationshipStore();
        PropertyStore propertyStore = neoStores.getPropertyStore();
        JobScheduler scheduler = JobSchedulerFactory.createScheduler();
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( EMPTY );
        DefaultIndexProviderMap providerMap = new DefaultIndexProviderMap( dependencies, config );
        life.add( providerMap );
        indexingService = IndexingServiceFactory.createIndexingService( config, scheduler, providerMap,
                new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE, neoStores ), SchemaUtil.idTokenNameLookup, empty(), nullLogProvider, nullLogProvider,
                IndexingService.NO_MONITOR, new DatabaseSchemaState( nullLogProvider ) );
        propertyPhysicalToLogicalConverter = new PropertyPhysicalToLogicalConverter( neoStores.getPropertyStore() );
        life.add( indexingService );
        life.add( scheduler );
        life.init();
        life.start();
        propertyCreator = new PropertyCreator( neoStores.getPropertyStore(), new PropertyTraverser() );
        recordAccess = new DirectRecordAccess<>( neoStores.getPropertyStore(), Loaders.propertyLoader( propertyStore ) );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
        neoStores.close();
    }

    @Test
    public void shouldContainFedNodeUpdate() throws Exception
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, relationshipStore, indexingService, propertyPhysicalToLogicalConverter );

        int nodeId = 0;
        NodeRecord inUse = getNode( nodeId, true );
        Value propertyValue = Values.of( "hej" );
        long propertyId = createNodeProperty( inUse, propertyValue, 1 );
        NodeRecord notInUse = getNode( nodeId, false );
        nodeStore.updateRecord( inUse );

        Command.NodeCommand nodeCommand = new Command.NodeCommand( inUse, notInUse );
        PropertyRecord propertyBlocks = new PropertyRecord( propertyId );
        propertyBlocks.setNodeId( nodeId );
        Command.PropertyCommand propertyCommand = new Command.PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), propertyBlocks );

        StoreIndexDescriptor indexDescriptor = forSchema( multiToken( ENTITY_TOKENS, NODE, 1, 4, 6 ), EMPTY.getProviderDescriptor() ).withId( 0 );
        indexingService.createIndexes( indexDescriptor );
        indexingService.getIndexProxy( indexDescriptor.schema() ).awaitStoreScanCompleted();

        onlineIndexUpdates.feed( LongObjectMaps.immutable.of( nodeId, singletonList( propertyCommand ) ), LongObjectMaps.immutable.empty(),
                LongObjectMaps.immutable.of( nodeId, nodeCommand ), LongObjectMaps.immutable.empty() );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        Iterator<IndexEntryUpdate<SchemaDescriptor>> iterator = onlineIndexUpdates.iterator();
        assertEquals( iterator.next(), IndexEntryUpdate.remove( nodeId, indexDescriptor, propertyValue, null, null ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldContainFedRelationshipUpdate() throws Exception
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, relationshipStore, indexingService, propertyPhysicalToLogicalConverter );

        long relId = 0;
        RelationshipRecord inUse = getRelationship( relId, true, ENTITY_TOKEN );
        Value propertyValue = Values.of( "hej" );
        long propertyId = createRelationshipProperty( inUse, propertyValue, 1 );
        RelationshipRecord notInUse = getRelationship( relId, false, ENTITY_TOKEN );
        relationshipStore.updateRecord( inUse );

        Command.RelationshipCommand relationshipCommand = new Command.RelationshipCommand( inUse, notInUse );
        PropertyRecord propertyBlocks = new PropertyRecord( propertyId );
        propertyBlocks.setRelId( relId );
        Command.PropertyCommand propertyCommand = new Command.PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), propertyBlocks );

        StoreIndexDescriptor indexDescriptor = forSchema( multiToken( ENTITY_TOKENS, RELATIONSHIP, 1, 4, 6 ), EMPTY.getProviderDescriptor() ).withId( 0 );
        indexingService.createIndexes( indexDescriptor );
        indexingService.getIndexProxy( indexDescriptor.schema() ).awaitStoreScanCompleted();

        onlineIndexUpdates.feed( LongObjectMaps.immutable.empty(), LongObjectMaps.immutable.of( relId, singletonList( propertyCommand ) ),
                LongObjectMaps.immutable.empty(), LongObjectMaps.immutable.of( relId, relationshipCommand ) );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        Iterator<IndexEntryUpdate<SchemaDescriptor>> iterator = onlineIndexUpdates.iterator();
        assertEquals( iterator.next(), IndexEntryUpdate.remove( relId, indexDescriptor, propertyValue, null, null ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldDifferentiateNodesAndRelationships() throws Exception
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, relationshipStore, indexingService, propertyPhysicalToLogicalConverter );

        int nodeId = 0;
        NodeRecord inUseNode = getNode( nodeId, true );
        Value nodePropertyValue = Values.of( "hej" );
        long nodePropertyId = createNodeProperty( inUseNode, nodePropertyValue, 1 );
        NodeRecord notInUseNode = getNode( nodeId, false );
        nodeStore.updateRecord( inUseNode );

        Command.NodeCommand nodeCommand = new Command.NodeCommand( inUseNode, notInUseNode );
        PropertyRecord nodePropertyBlocks = new PropertyRecord( nodePropertyId );
        nodePropertyBlocks.setNodeId( nodeId );
        Command.PropertyCommand nodePropertyCommand =
                new Command.PropertyCommand( recordAccess.getIfLoaded( nodePropertyId ).forReadingData(), nodePropertyBlocks );

        StoreIndexDescriptor nodeIndexDescriptor = forSchema( multiToken( ENTITY_TOKENS, NODE, 1, 4, 6 ), EMPTY.getProviderDescriptor() ).withId( 0 );
        indexingService.createIndexes( nodeIndexDescriptor );
        indexingService.getIndexProxy( nodeIndexDescriptor.schema() ).awaitStoreScanCompleted();

        long relId = 0;
        RelationshipRecord inUse = getRelationship( relId, true, ENTITY_TOKEN );
        Value relationshipPropertyValue = Values.of( "da" );
        long propertyId = createRelationshipProperty( inUse, relationshipPropertyValue, 1 );
        RelationshipRecord notInUse = getRelationship( relId, false, ENTITY_TOKEN );
        relationshipStore.updateRecord( inUse );

        Command.RelationshipCommand relationshipCommand = new Command.RelationshipCommand( inUse, notInUse );
        PropertyRecord relationshipPropertyBlocks = new PropertyRecord( propertyId );
        relationshipPropertyBlocks.setRelId( relId );
        Command.PropertyCommand relationshipPropertyCommand =
                new Command.PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), relationshipPropertyBlocks );

        StoreIndexDescriptor relationshipIndexDescriptor =
                forSchema( multiToken( ENTITY_TOKENS, RELATIONSHIP, 1, 4, 6 ), EMPTY.getProviderDescriptor() ).withId( 1 );
        indexingService.createIndexes( relationshipIndexDescriptor );
        indexingService.getIndexProxy( relationshipIndexDescriptor.schema() ).awaitStoreScanCompleted();

        onlineIndexUpdates.feed( LongObjectMaps.immutable.of( nodeId, singletonList( nodePropertyCommand ) ),
                LongObjectMaps.immutable.of( relId, singletonList( relationshipPropertyCommand ) ), LongObjectMaps.immutable.of( nodeId, nodeCommand ),
                LongObjectMaps.immutable.of( relId, relationshipCommand ) );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        assertThat( onlineIndexUpdates,
                containsInAnyOrder( IndexEntryUpdate.remove( relId, relationshipIndexDescriptor, relationshipPropertyValue, null, null ),
                        IndexEntryUpdate.remove( nodeId, nodeIndexDescriptor, nodePropertyValue, null, null ) ) );
    }

    @Test
    public void shouldUpdateCorrectIndexes() throws Exception
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, relationshipStore, indexingService, propertyPhysicalToLogicalConverter );

        long relId = 0;
        RelationshipRecord inUse = getRelationship( relId, true, ENTITY_TOKEN );
        Value propertyValue = Values.of( "hej" );
        Value propertyValue2 = Values.of( "da" );
        long propertyId = createRelationshipProperty( inUse, propertyValue, 1 );
        long propertyId2 = createRelationshipProperty( inUse, propertyValue2, 4 );
        RelationshipRecord notInUse = getRelationship( relId, false, ENTITY_TOKEN );
        relationshipStore.updateRecord( inUse );

        Command.RelationshipCommand relationshipCommand = new Command.RelationshipCommand( inUse, notInUse );
        PropertyRecord propertyBlocks = new PropertyRecord( propertyId );
        propertyBlocks.setRelId( relId );
        Command.PropertyCommand propertyCommand = new Command.PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), propertyBlocks );

        PropertyRecord propertyBlocks2 = new PropertyRecord( propertyId2 );
        propertyBlocks2.setRelId( relId );
        Command.PropertyCommand propertyCommand2 = new Command.PropertyCommand( recordAccess.getIfLoaded( propertyId2 ).forReadingData(), propertyBlocks2 );

        StoreIndexDescriptor indexDescriptor0 = forSchema( multiToken( ENTITY_TOKENS, RELATIONSHIP, 1, 4, 6 ), EMPTY.getProviderDescriptor() ).withId( 0 );
        StoreIndexDescriptor indexDescriptor1 = forSchema( multiToken( ENTITY_TOKENS, RELATIONSHIP, 2, 4, 6 ), EMPTY.getProviderDescriptor() ).withId( 1 );
        StoreIndexDescriptor indexDescriptor2 =
                forSchema( multiToken( new int[]{ENTITY_TOKEN, OTHER_ENTITY_TOKEN}, RELATIONSHIP, 1 ), EMPTY.getProviderDescriptor() ).withId( 2 );
        StoreIndexDescriptor indexDescriptor3 =
                forSchema( multiToken( new int[]{OTHER_ENTITY_TOKEN}, RELATIONSHIP, 1 ), EMPTY.getProviderDescriptor() ).withId( 3 );
        indexingService.createIndexes( indexDescriptor0, indexDescriptor1, indexDescriptor2 );
        indexingService.getIndexProxy( indexDescriptor0.schema() ).awaitStoreScanCompleted();
        indexingService.getIndexProxy( indexDescriptor1.schema() ).awaitStoreScanCompleted();
        indexingService.getIndexProxy( indexDescriptor2.schema() ).awaitStoreScanCompleted();

        onlineIndexUpdates.feed( LongObjectMaps.immutable.empty(), LongObjectMaps.immutable.of( relId, asList( propertyCommand, propertyCommand2 ) ),
                LongObjectMaps.immutable.empty(), LongObjectMaps.immutable.of( relId, relationshipCommand ) );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        assertThat( onlineIndexUpdates, containsInAnyOrder( IndexEntryUpdate.remove( relId, indexDescriptor0, propertyValue, propertyValue2, null ),
                IndexEntryUpdate.remove( relId, indexDescriptor1, null, propertyValue2, null ),
                IndexEntryUpdate.remove( relId, indexDescriptor2, propertyValue ) ) );
        assertThat( onlineIndexUpdates, not( containsInAnyOrder( indexDescriptor3 ) ) ); // This index is only for a different relationship type.
    }

    private long createRelationshipProperty( RelationshipRecord relRecord, Value propertyValue, int propertyKey )
    {
        return propertyCreator.createPropertyChain( relRecord, singletonList( propertyCreator.encodePropertyValue( propertyKey, propertyValue ) ).iterator(),
                recordAccess );
    }

    private long createNodeProperty( NodeRecord inUse, Value value, int propertyKey )
    {
        return propertyCreator.createPropertyChain( inUse, singletonList( propertyCreator.encodePropertyValue( propertyKey, value ) ).iterator(),
                recordAccess );
    }

    private NodeRecord getNode( int nodeId, boolean inUse )
    {
        NodeRecord nodeRecord = new NodeRecord( nodeId );
        nodeRecord = nodeRecord.initialize( inUse, NO_NEXT_PROPERTY.longValue(), false, NO_NEXT_RELATIONSHIP.longValue(), NO_LABELS_FIELD.longValue() );
        InlineNodeLabels labelFieldWriter = new InlineNodeLabels( nodeRecord );
        labelFieldWriter.put( new long[]{ENTITY_TOKEN}, null, null );
        return nodeRecord;
    }

    private RelationshipRecord getRelationship( long relId, boolean inUse, int type )
    {
        return new RelationshipRecord( relId ).initialize( inUse, NO_NEXT_PROPERTY.longValue(), 0, 0, type, NO_NEXT_RELATIONSHIP.longValue(),
                NO_NEXT_RELATIONSHIP.longValue(), NO_NEXT_RELATIONSHIP.longValue(), NO_NEXT_RELATIONSHIP.longValue(), true, false );
    }
}
