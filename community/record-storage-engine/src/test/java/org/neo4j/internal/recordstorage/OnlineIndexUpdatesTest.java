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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsStore;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.schema.SchemaDescriptor.fulltext;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

@PageCacheExtension
@Neo4jLayoutExtension
class OnlineIndexUpdatesTest
{
    private static final int ENTITY_TOKEN = 1;
    private static final int OTHER_ENTITY_TOKEN = 2;
    private static final int[] ENTITY_TOKENS = {ENTITY_TOKEN};

    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    private NodeStore nodeStore;
    private RelationshipStore relationshipStore;
    private SchemaCache schemaCache;
    private PropertyPhysicalToLogicalConverter propertyPhysicalToLogicalConverter;
    private NeoStores neoStores;
    private LifeSupport life;
    private PropertyCreator propertyCreator;
    private DirectRecordAccess<PropertyRecord,PrimitiveRecord> recordAccess;

    @BeforeEach
    void setUp() throws IOException
    {
        life = new LifeSupport();
        Config config = Config.defaults();
        NullLogProvider nullLogProvider = NullLogProvider.getInstance();
        StoreFactory storeFactory =
                new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fileSystem, immediate() ), pageCache, fileSystem,
                        nullLogProvider );

        neoStores = storeFactory.openAllNeoStores( true );
        GBPTreeCountsStore counts = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), immediate(),
                new CountsComputer( neoStores, pageCache, databaseLayout ), false, GBPTreeCountsStore.NO_MONITOR );
        life.add( wrapInLifecycle( counts ) );
        nodeStore = neoStores.getNodeStore();
        relationshipStore = neoStores.getRelationshipStore();
        PropertyStore propertyStore = neoStores.getPropertyStore();

        schemaCache = new SchemaCache( new StandardConstraintRuleAccessor(), index -> index );
        propertyPhysicalToLogicalConverter = new PropertyPhysicalToLogicalConverter( neoStores.getPropertyStore() );
        life.start();
        propertyCreator = new PropertyCreator( neoStores.getPropertyStore(), new PropertyTraverser() );
        recordAccess = new DirectRecordAccess<>( neoStores.getPropertyStore(), Loaders.propertyLoader( propertyStore ) );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
        neoStores.close();
    }

    @Test
    void shouldContainFedNodeUpdate()
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, schemaCache, propertyPhysicalToLogicalConverter,
                new RecordStorageReader( neoStores ) );

        int nodeId = 0;
        NodeRecord inUse = getNode( nodeId, true );
        Value propertyValue = Values.of( "hej" );
        long propertyId = createNodeProperty( inUse, propertyValue, 1 );
        NodeRecord notInUse = getNode( nodeId, false );
        nodeStore.updateRecord( inUse );

        NodeCommand nodeCommand = new NodeCommand( inUse, notInUse );
        PropertyRecord propertyBlocks = new PropertyRecord( propertyId );
        propertyBlocks.setNodeId( nodeId );
        PropertyCommand propertyCommand = new PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), propertyBlocks );

        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                fulltext( NODE, ENTITY_TOKENS, new int[]{1, 4, 6} ) ).withName( "index" ).materialise( 0 );
        createIndexes( indexDescriptor );

        onlineIndexUpdates.feed( nodeGroup( nodeCommand, propertyCommand ), relationshipGroup( null ) );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        Iterator<IndexEntryUpdate<IndexDescriptor>> iterator = onlineIndexUpdates.iterator();
        assertEquals( iterator.next(), IndexEntryUpdate.remove( nodeId, indexDescriptor, propertyValue, null, null ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldContainFedRelationshipUpdate()
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, schemaCache, propertyPhysicalToLogicalConverter,
                new RecordStorageReader( neoStores ) );

        long relId = 0;
        RelationshipRecord inUse = getRelationship( relId, true, ENTITY_TOKEN );
        Value propertyValue = Values.of( "hej" );
        long propertyId = createRelationshipProperty( inUse, propertyValue, 1 );
        RelationshipRecord notInUse = getRelationship( relId, false, ENTITY_TOKEN );
        relationshipStore.updateRecord( inUse );

        Command.RelationshipCommand relationshipCommand = new Command.RelationshipCommand( inUse, notInUse );
        PropertyRecord propertyBlocks = new PropertyRecord( propertyId );
        propertyBlocks.setRelId( relId );
        PropertyCommand propertyCommand = new PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), propertyBlocks );

        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                fulltext( RELATIONSHIP, ENTITY_TOKENS, new int[]{1, 4, 6} ) ).withName( "index" ).materialise( 0 );
        createIndexes( indexDescriptor );

        onlineIndexUpdates.feed( nodeGroup( null ), relationshipGroup( relationshipCommand, propertyCommand ) );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        Iterator<IndexEntryUpdate<IndexDescriptor>> iterator = onlineIndexUpdates.iterator();
        assertEquals( iterator.next(), IndexEntryUpdate.remove( relId, indexDescriptor, propertyValue, null, null ) );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldDifferentiateNodesAndRelationships()
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, schemaCache, propertyPhysicalToLogicalConverter,
                new RecordStorageReader( neoStores ) );

        int nodeId = 0;
        NodeRecord inUseNode = getNode( nodeId, true );
        Value nodePropertyValue = Values.of( "hej" );
        long nodePropertyId = createNodeProperty( inUseNode, nodePropertyValue, 1 );
        NodeRecord notInUseNode = getNode( nodeId, false );
        nodeStore.updateRecord( inUseNode );

        NodeCommand nodeCommand = new NodeCommand( inUseNode, notInUseNode );
        PropertyRecord nodePropertyBlocks = new PropertyRecord( nodePropertyId );
        nodePropertyBlocks.setNodeId( nodeId );
        PropertyCommand nodePropertyCommand =
                new PropertyCommand( recordAccess.getIfLoaded( nodePropertyId ).forReadingData(), nodePropertyBlocks );

        IndexDescriptor nodeIndexDescriptor = IndexPrototype.forSchema(
                fulltext( NODE, ENTITY_TOKENS, new int[]{1, 4, 6} ) ).withName( "index" ).materialise( 0 );
        createIndexes( nodeIndexDescriptor );

        long relId = 0;
        RelationshipRecord inUse = getRelationship( relId, true, ENTITY_TOKEN );
        Value relationshipPropertyValue = Values.of( "da" );
        long propertyId = createRelationshipProperty( inUse, relationshipPropertyValue, 1 );
        RelationshipRecord notInUse = getRelationship( relId, false, ENTITY_TOKEN );
        relationshipStore.updateRecord( inUse );

        Command.RelationshipCommand relationshipCommand = new Command.RelationshipCommand( inUse, notInUse );
        PropertyRecord relationshipPropertyBlocks = new PropertyRecord( propertyId );
        relationshipPropertyBlocks.setRelId( relId );
        PropertyCommand relationshipPropertyCommand =
                new PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), relationshipPropertyBlocks );

        FulltextSchemaDescriptor schema = fulltext( RELATIONSHIP, ENTITY_TOKENS, new int[]{1, 4, 6} );
        IndexDescriptor relationshipIndexDescriptor = IndexPrototype.forSchema( schema ).withName( "index" ).materialise( 1 );
        createIndexes( relationshipIndexDescriptor );

        onlineIndexUpdates.feed( nodeGroup( nodeCommand, nodePropertyCommand ), relationshipGroup( relationshipCommand, relationshipPropertyCommand ) );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        assertThat( onlineIndexUpdates ).contains( IndexEntryUpdate.remove( relId, relationshipIndexDescriptor, relationshipPropertyValue, null, null ),
                IndexEntryUpdate.remove( nodeId, nodeIndexDescriptor, nodePropertyValue, null, null ) );
    }

    @Test
    void shouldUpdateCorrectIndexes()
    {
        OnlineIndexUpdates onlineIndexUpdates = new OnlineIndexUpdates( nodeStore, schemaCache, propertyPhysicalToLogicalConverter,
                new RecordStorageReader( neoStores ) );

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
        PropertyCommand propertyCommand = new PropertyCommand( recordAccess.getIfLoaded( propertyId ).forReadingData(), propertyBlocks );

        PropertyRecord propertyBlocks2 = new PropertyRecord( propertyId2 );
        propertyBlocks2.setRelId( relId );
        PropertyCommand propertyCommand2 = new PropertyCommand( recordAccess.getIfLoaded( propertyId2 ).forReadingData(), propertyBlocks2 );

        IndexDescriptor indexDescriptor0 = IndexPrototype.forSchema(
                fulltext( RELATIONSHIP, ENTITY_TOKENS, new int[]{1, 4, 6} ) ).withName( "index_0" ).materialise( 0 );
        IndexDescriptor indexDescriptor1 = IndexPrototype.forSchema(
                fulltext( RELATIONSHIP, ENTITY_TOKENS, new int[]{2, 4, 6} ) ).withName( "index_1" ).materialise( 1 );
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema(
                fulltext( RELATIONSHIP, new int[]{ENTITY_TOKEN, OTHER_ENTITY_TOKEN}, new int[]{1} ) )
                .withName( "index_2" ).materialise( 2 );
        createIndexes( indexDescriptor0, indexDescriptor1, indexDescriptor );

        onlineIndexUpdates.feed( nodeGroup( null ), relationshipGroup( relationshipCommand, propertyCommand, propertyCommand2 ) );
        assertTrue( onlineIndexUpdates.hasUpdates() );
        assertThat( onlineIndexUpdates ).contains( IndexEntryUpdate.remove( relId, indexDescriptor0, propertyValue, propertyValue2, null ),
                IndexEntryUpdate.remove( relId, indexDescriptor1, null, propertyValue2, null ),
                IndexEntryUpdate.remove( relId, indexDescriptor, propertyValue ) );
    }

    private void createIndexes( IndexDescriptor... indexDescriptors )
    {
        for ( IndexDescriptor indexDescriptor : indexDescriptors )
        {
            schemaCache.addSchemaRule( indexDescriptor );
        }
    }

    private EntityCommandGrouper<NodeCommand>.Cursor nodeGroup( NodeCommand nodeCommand, PropertyCommand... propertyCommands )
    {
        return group( nodeCommand, NodeCommand.class, propertyCommands );
    }

    private EntityCommandGrouper<Command.RelationshipCommand>.Cursor relationshipGroup( Command.RelationshipCommand relationshipCommand,
            PropertyCommand... propertyCommands )
    {
        return group( relationshipCommand, Command.RelationshipCommand.class, propertyCommands );
    }

    private <ENTITY extends Command> EntityCommandGrouper<ENTITY>.Cursor group( ENTITY entityCommand, Class<ENTITY> cls,
            PropertyCommand... propertyCommands )
    {
        EntityCommandGrouper<ENTITY> grouper = new EntityCommandGrouper<>( cls, 8 );
        if ( entityCommand != null )
        {
            grouper.add( entityCommand );
        }
        for ( PropertyCommand propertyCommand : propertyCommands )
        {
            grouper.add( propertyCommand );
        }
        return grouper.sortAndAccessGroups();
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
        if ( inUse )
        {
            InlineNodeLabels labelFieldWriter = new InlineNodeLabels( nodeRecord );
            labelFieldWriter.put( new long[]{ENTITY_TOKEN}, null, null );
        }
        return nodeRecord;
    }

    private RelationshipRecord getRelationship( long relId, boolean inUse, int type )
    {
        if ( !inUse )
        {
            type = -1;
        }
        return new RelationshipRecord( relId ).initialize( inUse, NO_NEXT_PROPERTY.longValue(), 0, 0, type, NO_NEXT_RELATIONSHIP.longValue(),
                NO_NEXT_RELATIONSHIP.longValue(), NO_NEXT_RELATIONSHIP.longValue(), NO_NEXT_RELATIONSHIP.longValue(), true, false );
    }

    private static Lifecycle wrapInLifecycle( CountsStore countsStore )
    {
        return new LifecycleAdapter()
        {
            @Override
            public void start() throws IOException
            {
                countsStore.start();
            }

            @Override
            public void shutdown()
            {
                countsStore.close();
            }
        };
    }
}
