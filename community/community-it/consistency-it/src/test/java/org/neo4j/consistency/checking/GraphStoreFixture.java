/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.checking;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.iterator.LongIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.LookupAccessorsFromRunningDb;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.util.Preconditions;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.recordstorage.StoreTokens.allReadableTokens;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public abstract class GraphStoreFixture implements AutoCloseable
{
    private DirectStoreAccess directStoreAccess;
    private final long[] highIds = new long[StoreType.values().length];

    /**
     * Record format used to generate initial database.
     */
    private final String formatName;
    private PageCache pageCache;
    private final TestDirectory testDirectory;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI database;
    private InternalTransactionCommitProcess commitProcess;
    private TransactionIdStore transactionIdStore;
    private NeoStores neoStores;
    private IndexingService indexingService;
    private RecordStorageEngine storageEngine;
    private CountsAccessor countsStore;
    private RelationshipGroupDegreesStore groupDegreesStore;

    protected GraphStoreFixture( String formatName, TestDirectory testDirectory )
    {
        this.formatName = formatName;
        this.testDirectory = testDirectory;
        startDatabaseAndExtractComponents();
        generateInitialData();
    }

    private void startDatabaseAndExtractComponents()
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setFileSystem( testDirectory.getFileSystem() )
                .setConfig( GraphDatabaseSettings.record_format, formatName )
                // Some tests using this fixture were written when the label_block_size was 60 and so hardcoded
                // tests and records around that. Those tests could change, but the simpler option is to just
                // keep the block size to 60 and let them be.
                .setConfig( GraphDatabaseInternalSettings.label_block_size, 60 )
                .setConfig( GraphDatabaseInternalSettings.consistency_check_on_apply, false )
                .setConfig( getConfig() )
                .build();
        database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        DependencyResolver dependencyResolver = database.getDependencyResolver();

        commitProcess = new InternalTransactionCommitProcess(
                dependencyResolver.resolveDependency( TransactionAppender.class ),
                dependencyResolver.resolveDependency( StorageEngine.class ) );
        transactionIdStore = database.getDependencyResolver().resolveDependency(
                TransactionIdStore.class );

        storageEngine = dependencyResolver.resolveDependency( RecordStorageEngine.class );
        neoStores = storageEngine.testAccessNeoStores();
        indexingService = dependencyResolver.resolveDependency( IndexingService.class );
        directStoreAccess = new DirectStoreAccess( neoStores,
                dependencyResolver.resolveDependency( IndexProviderMap.class ),
                dependencyResolver.resolveDependency( TokenHolders.class ),
                dependencyResolver.resolveDependency( IndexStatisticsStore.class ),
                dependencyResolver.resolveDependency( IdGeneratorFactory.class ) );
        countsStore = storageEngine.countsAccessor();
        groupDegreesStore = storageEngine.relationshipGroupDegreesStore();
        pageCache = dependencyResolver.resolveDependency( PageCache.class );

    }

    @Override
    public void close()
    {
        managementService.shutdown();
    }

    public void apply( Transaction transaction ) throws KernelException
    {
        TransactionRepresentation representation =
                transaction.representation( idGenerator(), transactionIdStore.getLastCommittedTransactionId(), neoStores, indexingService );
        commitProcess.commit( new TransactionToApply( representation, NULL ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
    }

    public void apply( Consumer<org.neo4j.graphdb.Transaction> tx )
    {
        try ( org.neo4j.graphdb.Transaction transaction = database.beginTx() )
        {
            tx.accept( transaction );
            transaction.commit();
        }
    }

    public DirectStoreAccess directStoreAccess()
    {
        return directStoreAccess( false );
    }

    public DirectStoreAccess readOnlyDirectStoreAccess()
    {
        return directStoreAccess( false );
    }

    public PageCache getInstantiatedPageCache()
    {
        return pageCache;
    }

    private DirectStoreAccess directStoreAccess( boolean readOnly )
    {
        Preconditions.checkState( !readOnly, "Doesn't support read-only yet" );
        return directStoreAccess;
    }

    public ThrowingSupplier<CountsStore,IOException> counts()
    {
        return () -> (CountsStore) countsStore;
    }

    public ThrowingSupplier<RelationshipGroupDegreesStore,IOException> groupDegrees()
    {
        return () -> groupDegreesStore;
    }

    /**
     * Accessors from this IndexAccessorLookup are taken from the running database and should not be closed.
     */
    public IndexAccessors.IndexAccessorLookup indexAccessorLookup()
    {
        return new LookupAccessorsFromRunningDb( indexingService );
    }

    public DatabaseLayout databaseLayout()
    {
        return Neo4jLayout.of( testDirectory.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
    }

    public IndexingService indexingService()
    {
        return indexingService;
    }

    public EntityUpdates nodeAsUpdates( long nodeId )
    {
        try ( StorageReader storeReader = storageEngine.newReader();
              StorageNodeCursor nodeCursor = storeReader.allocateNodeCursor( NULL );
              StoragePropertyCursor propertyCursor = storeReader.allocatePropertyCursor( NULL, INSTANCE ) )
        {
            nodeCursor.single( nodeId );
            long[] labels;
            if ( !nodeCursor.next() || !nodeCursor.hasProperties() || (labels = nodeCursor.labels()).length == 0 )
            {
                return null;
            }
            nodeCursor.properties( propertyCursor );
            EntityUpdates.Builder update = EntityUpdates.forEntity( nodeId, true ).withTokens( labels );
            while ( propertyCursor.next() )
            {
                update.added( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
            }
            return update.build();
        }
    }

    public EntityUpdates relationshipAsUpdates( long relId )
    {
        try ( StorageReader storeReader = storageEngine.newReader();
              StorageRelationshipScanCursor relCursor = storeReader.allocateRelationshipScanCursor( NULL );
              StorageNodeCursor nodeCursor = storeReader.allocateNodeCursor( NULL );
              StoragePropertyCursor propertyCursor = storeReader.allocatePropertyCursor( NULL, INSTANCE ) )
        {
            relCursor.single( relId );
            if ( !relCursor.next() || !relCursor.hasProperties() )
            {
                return null;
            }
            int type = relCursor.type();
            relCursor.properties( propertyCursor );
            EntityUpdates.Builder update = EntityUpdates.forEntity( relId, true ).withTokens( type );
            while ( propertyCursor.next() )
            {
                update.added( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
            }
            return update.build();
        }
    }

    public Iterator<IndexDescriptor> getIndexDescriptors()
    {
        LongIterator ids = indexingService.getIndexIds().longIterator();
        return new PrefetchingIterator<>()
        {
            @Override
            protected IndexDescriptor fetchNextOrNull()
            {
                if ( ids.hasNext() )
                {
                    long indexId = ids.next();
                    try
                    {
                        return indexingService.getIndexProxy( indexId ).getDescriptor();
                    }
                    catch ( IndexNotFoundKernelException e )
                    {
                        throw new IllegalStateException( e );
                    }
                }
                return null;
            }
        };
    }

    public NeoStores neoStores()
    {
        return neoStores;
    }

    @FunctionalInterface
    interface TokenChange
    {
        int createToken( String name, boolean internal, TransactionDataBuilder tx, IdGenerator next );
    }

    public TokenHolders writableTokenHolders()
    {
        TokenHolder propertyKeyTokens = new DelegatingTokenHolder( buildTokenCreator( ( name, internal, tx, next ) ->
        {
            int id = next.propertyKey();
            tx.propertyKey( id, name, internal );
            return id;
        } ), TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolder labelTokens = new DelegatingTokenHolder( buildTokenCreator( ( name, internal, tx, next ) ->
        {
            int id = next.label();
            tx.nodeLabel( id, name, internal );
            return id;
        } ), TokenHolder.TYPE_LABEL );
        TokenHolder relationshipTypeTokens = new DelegatingTokenHolder( buildTokenCreator( ( name, internal, tx, next ) ->
        {
            int id = next.relationshipType();
            tx.relationshipType( id, name, internal );
            return id;
        } ), TokenHolder.TYPE_RELATIONSHIP_TYPE );
        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokens, labelTokens, relationshipTypeTokens );
        tokenHolders.setInitialTokens( allReadableTokens( directStoreAccess().nativeStores() ), NULL );
        return tokenHolders;
    }

    private TokenCreator buildTokenCreator( TokenChange propChange )
    {
        return ( name, internal ) ->
        {
            MutableInt keyId = new MutableInt();
            apply( new Transaction()
            {
                @Override
                protected void transactionData( TransactionDataBuilder tx, IdGenerator next )
                {
                    keyId.setValue( propChange.createToken( name, internal, tx, next ) );
                }
            } );
            return keyId.intValue();
        };
    }

    public abstract static class Transaction
    {
        final long startTimestamp = currentTimeMillis();

        protected abstract void transactionData( TransactionDataBuilder tx, IdGenerator next ) throws KernelException;

        public TransactionRepresentation representation( IdGenerator idGenerator, long lastCommittedTx, NeoStores neoStores, IndexingService indexingService )
                throws KernelException
        {
            TransactionWriter writer = new TransactionWriter( neoStores );
            transactionData( new TransactionDataBuilder( writer, neoStores, idGenerator, indexingService ), idGenerator );
            idGenerator.updateCorrespondingIdGenerators( neoStores );
            return writer.representation( new byte[0], startTimestamp, lastCommittedTx, currentTimeMillis() );
        }
    }

    public IdGenerator idGenerator()
    {
        return new IdGenerator();
    }

    public class IdGenerator
    {
        private long nextId( StoreType type )
        {
            return highIds[type.ordinal()]++;
        }

        public long schema()
        {
            return nextId( StoreType.SCHEMA );
        }

        public long node()
        {
            return nextId( StoreType.NODE );
        }

        public int label()
        {
            return (int) nextId( StoreType.LABEL_TOKEN );
        }

        public long nodeLabel()
        {
            return nextId( StoreType.NODE_LABEL );
        }

        public long relationship()
        {
            return nextId( StoreType.RELATIONSHIP );
        }

        public long relationshipGroup()
        {
            return nextId( StoreType.RELATIONSHIP_GROUP );
        }

        public long property()
        {
            return nextId( StoreType.PROPERTY );
        }

        public long stringProperty()
        {
            return nextId( StoreType.PROPERTY_STRING );
        }

        public long arrayProperty()
        {
            return nextId( StoreType.PROPERTY_ARRAY );
        }

        public int relationshipType()
        {
            return (int) nextId( StoreType.RELATIONSHIP_TYPE_TOKEN );
        }

        public int propertyKey()
        {
            return (int) nextId( StoreType.PROPERTY_KEY_TOKEN );
        }

        void updateCorrespondingIdGenerators( NeoStores neoStores )
        {
            neoStores.getNodeStore().setHighestPossibleIdInUse( highIds[StoreType.NODE.ordinal()] );
            neoStores.getRelationshipStore().setHighestPossibleIdInUse( highIds[StoreType.RELATIONSHIP.ordinal()] );
            neoStores.getRelationshipGroupStore().setHighestPossibleIdInUse( highIds[StoreType.RELATIONSHIP_GROUP.ordinal()] );
        }
    }

    public static final class TransactionDataBuilder
    {
        private final TransactionWriter writer;
        private final NodeStore nodes;
        private final IndexingService indexingService;
        private final TokenHolders tokenHolders;
        private final AtomicInteger propKeyDynIds;
        private final AtomicInteger labelDynIds;
        private final AtomicInteger relTypeDynIds;

        TransactionDataBuilder( TransactionWriter writer, NeoStores neoStores, IdGenerator next, IndexingService indexingService )
        {
            this.propKeyDynIds = new AtomicInteger( (int) neoStores.getPropertyKeyTokenStore().getNameStore().getHighId() );
            this.labelDynIds = new AtomicInteger( (int) neoStores.getLabelTokenStore().getNameStore().getHighId() );
            this.relTypeDynIds = new AtomicInteger( (int) neoStores.getRelationshipTypeTokenStore().getNameStore().getHighId() );
            this.writer = writer;
            this.nodes = neoStores.getNodeStore();
            this.indexingService = indexingService;

            TokenHolder propTokens = new DelegatingTokenHolder( ( name, internal ) ->
            {
                int id = next.propertyKey();
                writer.propertyKey( id, name, internal, dynIds( 0, propKeyDynIds, name ) );
                return id;
            }, TokenHolder.TYPE_PROPERTY_KEY );

            TokenHolder labelTokens = new DelegatingTokenHolder( ( name, internal ) ->
            {
                int id = next.label();
                writer.label( id, name, internal, dynIds( 0, labelDynIds, name ) );
                return id;
            }, TokenHolder.TYPE_LABEL );

            TokenHolder relTypeTokens = new DelegatingTokenHolder( ( name, internal ) ->
            {
                int id = next.relationshipType();
                writer.relationshipType( id, name, internal, dynIds( 0, relTypeDynIds, name ) );
                return id;
            }, TokenHolder.TYPE_RELATIONSHIP_TYPE );

            this.tokenHolders = new TokenHolders( propTokens, labelTokens, relTypeTokens );
            tokenHolders.setInitialTokens( allReadableTokens( neoStores ), NULL );
            tokenHolders.propertyKeyTokens().getAllTokens().forEach( token -> propKeyDynIds.getAndUpdate( id -> Math.max( id, token.id() + 1 ) ) );
            tokenHolders.labelTokens().getAllTokens().forEach( token -> labelDynIds.getAndUpdate( id -> Math.max( id, token.id() + 1 ) ) );
            tokenHolders.relationshipTypeTokens().getAllTokens().forEach( token -> relTypeDynIds.getAndUpdate( id -> Math.max( id, token.id() + 1 ) ) );
        }

        private static int[] dynIds( int externalBase, AtomicInteger idGenerator, String name )
        {
            if ( idGenerator.get() <= externalBase )
            {
                idGenerator.set( externalBase + 1 );
            }
            byte[] bytes = name.getBytes( StandardCharsets.UTF_8 );
            int blocks = 1 + ( bytes.length / TokenStore.NAME_STORE_BLOCK_SIZE );
            int base = idGenerator.getAndAdd( blocks );
            int[] ids = new int[blocks];
            for ( int i = 0; i < blocks; i++ )
            {
                ids[i] = base + i;
            }
            return ids;
        }

        public TokenHolders tokenHolders()
        {
            return tokenHolders;
        }

        public void createSchema( SchemaRecord before, SchemaRecord after, SchemaRule rule )
        {
            writer.createSchema( before, after, rule );
        }

        public int[] propertyKey( int id, String key, boolean internal )
        {
            int[] dynamicIds = dynIds( id, propKeyDynIds, key );
            writer.propertyKey( id, key, internal, dynamicIds );
            tokenHolders.propertyKeyTokens().addToken( new NamedToken( key, id ) );
            return dynamicIds;
        }

        public int[] nodeLabel( int id, String name, boolean internal )
        {
            int[] dynamicIds = dynIds( id, labelDynIds, name );
            writer.label( id, name, internal, dynamicIds );
            tokenHolders.labelTokens().addToken( new NamedToken( name, id ) );
            return dynamicIds;
        }

        public int[] relationshipType( int id, String relationshipType, boolean internal )
        {
            int[] dynamicIds = dynIds( id, relTypeDynIds, relationshipType );
            writer.relationshipType( id, relationshipType, internal, dynamicIds );
            tokenHolders.relationshipTypeTokens().addToken( new NamedToken( relationshipType, id ) );
            return dynamicIds;
        }

        public void create( NodeRecord node )
        {
            updateCounts( node, 1 );
            writer.create( node );
        }

        public void update( NodeRecord before, NodeRecord after )
        {
            updateCounts( before, -1 );
            updateCounts( after, 1 );
            writer.update( before, after );
        }

        public void delete( NodeRecord node )
        {
            updateCounts( node, -1 );
            writer.delete( node );
        }

        public void create( RelationshipRecord relationship )
        {
            writer.create( relationship );
        }

        public void update( RelationshipRecord before, RelationshipRecord after )
        {
            writer.update( before, after );
        }

        public void delete( RelationshipRecord relationship )
        {
            writer.delete( relationship );
        }

        public void create( RelationshipGroupRecord group )
        {
            writer.create( group );
        }

        public void update( RelationshipGroupRecord before, RelationshipGroupRecord after )
        {
            writer.update( before, after );
        }

        public void delete( RelationshipGroupRecord group )
        {
            writer.delete( group );
        }

        public void create( PropertyRecord property )
        {
            writer.create( property );
        }

        public void update( PropertyRecord before, PropertyRecord property )
        {
            writer.update( before, property );
        }

        public void delete( PropertyRecord before, PropertyRecord property )
        {
            writer.delete( before, property );
        }

        private void updateCounts( NodeRecord node, int delta )
        {
            writer.incrementNodeCount( ANY_LABEL, delta );
            for ( long label : NodeLabelsField.parseLabelsField( node ).get( nodes, NULL ) )
            {
                writer.incrementNodeCount( (int)label, delta );
            }
        }

        public void incrementNodeCount( int labelId, long delta )
        {
            writer.incrementNodeCount( labelId, delta );
        }

        public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
        {
            writer.incrementRelationshipCount( startLabelId, typeId, endLabelId, delta );
        }

        public IndexDescriptor completeConfiguration( IndexDescriptor indexDescriptor )
        {
            return indexingService.completeConfiguration( indexDescriptor );
        }
    }

    protected abstract void generateInitialData( GraphDatabaseService graphDb );

    private void generateInitialData()
    {
        generateInitialData( database );
        keepHighId( StoreType.SCHEMA, neoStores.getSchemaStore() );
        keepHighId( StoreType.NODE, neoStores.getNodeStore() );
        keepHighId( StoreType.LABEL_TOKEN, neoStores.getLabelTokenStore() );
        keepHighId( StoreType.NODE_LABEL, neoStores.getNodeStore().getDynamicLabelStore() );
        keepHighId( StoreType.RELATIONSHIP, neoStores.getRelationshipStore() );
        keepHighId( StoreType.RELATIONSHIP_GROUP, neoStores.getRelationshipGroupStore() );
        keepHighId( StoreType.PROPERTY, neoStores.getPropertyStore() );
        keepHighId( StoreType.PROPERTY_STRING, neoStores.getPropertyStore().getStringStore() );
        keepHighId( StoreType.PROPERTY_ARRAY, neoStores.getPropertyStore().getArrayStore() );
        keepHighId( StoreType.RELATIONSHIP_TYPE_TOKEN, neoStores.getRelationshipTypeTokenStore() );
        keepHighId( StoreType.PROPERTY_KEY_TOKEN, neoStores.getPropertyKeyTokenStore() );
    }

    private void keepHighId( StoreType storeType, RecordStore<? extends AbstractBaseRecord> store )
    {
        highIds[storeType.ordinal()] = store.getHighId();
    }

    protected abstract Map<Setting<?>, Object> getConfig();
}
