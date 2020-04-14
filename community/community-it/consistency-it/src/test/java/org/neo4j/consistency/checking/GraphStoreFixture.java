/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.statistics.AccessStatistics;
import org.neo4j.consistency.statistics.AccessStatsKeepingStoreAccess;
import org.neo4j.consistency.statistics.DefaultCounts;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.consistency.statistics.VerboseStatistics;
import org.neo4j.consistency.store.DirectStoreAccess;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.TokenScanStore;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.consistency.internal.SchemaIndexExtensionLoader.instantiateExtensions;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.GBPTreeCountsStore.NO_MONITOR;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.recordstorage.StoreTokens.allReadableTokens;
import static org.neo4j.internal.recordstorage.StoreTokens.readOnlyTokenHolders;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public abstract class GraphStoreFixture implements AutoCloseable
{
    private DirectStoreAccess directStoreAccess;
    private Statistics statistics;
    private final boolean keepStatistics;
    private NeoStores neoStore;
    private StorageReader storeReader;
    private long schemaId;
    private long nodeId;
    private int labelId;
    private long nodeLabelsId;
    private long relId;
    private long relGroupId;
    private int propId;
    private long stringPropId;
    private long arrayPropId;
    private int relTypeId;
    private int propKeyId;
    private DefaultFileSystemAbstraction fileSystem;
    private final LifeSupport storeLife = new LifeSupport();
    private final LifeSupport fixtureLife = new LifeSupport();

    /**
     * Record format used to generate initial database.
     */
    private String formatName;
    private final PageCache pageCache;
    private final TestDirectory testDirectory;
    private LabelScanStore labelScanStore;
    private IndexStatisticsStore indexStatisticsStore;
    private ThreadPoolJobScheduler jobScheduler;
    private CountsStore counts;

    private GraphStoreFixture( boolean keepStatistics, String formatName, PageCache pageCache, TestDirectory testDirectory )
    {
        this.keepStatistics = keepStatistics;
        this.formatName = formatName;
        this.pageCache = pageCache;
        this.testDirectory = testDirectory;
        generateInitialData();
    }

    protected GraphStoreFixture( String formatName, PageCache pageCache, TestDirectory testDirectory )
    {
        this( false, formatName, pageCache, testDirectory );
    }

    @Override
    public void close() throws Exception
    {
        stop();
        storeLife.shutdown();
        fixtureLife.shutdown();
        IOUtils.closeAllSilently( fileSystem );
    }

    public void apply( Transaction transaction ) throws KernelException
    {
        applyTransaction( transaction );
    }

    public DirectStoreAccess directStoreAccess()
    {
        return directStoreAccess( false );
    }

    public DirectStoreAccess readOnlyDirectStoreAccess()
    {
        return directStoreAccess( true );
    }

    public PageCache getInstantiatedPageCache()
    {
        return pageCache;
    }

    private DirectStoreAccess directStoreAccess( boolean readOnly )
    {
        if ( directStoreAccess == null )
        {
            fileSystem = new DefaultFileSystemAbstraction();
            jobScheduler = new ThreadPoolJobScheduler( "Fixture-" );
            LogProvider logProvider = NullLogProvider.getInstance();
            Config config = Config.defaults( GraphDatabaseSettings.read_only, readOnly );
            DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
            StoreFactory storeFactory = new StoreFactory( databaseLayout(), config, idGeneratorFactory, pageCache, fileSystem, logProvider,
                    PageCacheTracer.NULL );
            neoStore = storeFactory.openAllNeoStores();
            StoreAccess nativeStores;
            if ( keepStatistics )
            {
                AccessStatistics accessStatistics = new AccessStatistics();
                statistics = new VerboseStatistics( accessStatistics,
                        new DefaultCounts( defaultConsistencyCheckThreadsNumber() ), NullLog.getInstance() );
                nativeStores = new AccessStatsKeepingStoreAccess( neoStore, accessStatistics );
            }
            else
            {
                statistics = Statistics.NONE;
                nativeStores = new StoreAccess( neoStore );
            }
            nativeStores.initialize();
            fixtureLife.start();
            storeLife.start();

            IndexStoreView indexStoreView = new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE,
                    () -> new RecordStorageReader( nativeStores.getRawNeoStores() ) );

            Monitors monitors = new Monitors();
            labelScanStore = startLabelScanStore( pageCache, indexStoreView, monitors, readOnly );
            IndexProviderMap indexes = createIndexes( pageCache, config, logProvider, monitors);
            indexStatisticsStore = startIndexStatisticsStore( readOnly );
            directStoreAccess = new DirectStoreAccess( nativeStores, labelScanStore, indexes, readOnlyTokenHolders( neoStore, NULL ),
                    indexStatisticsStore, idGeneratorFactory );
            storeReader = new RecordStorageReader( neoStore );
        }
        return directStoreAccess;
    }

    private IndexStatisticsStore startIndexStatisticsStore( boolean readOnly )
    {
        final IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout(), immediate(), readOnly, PageCacheTracer.NULL );
        try
        {
            indexStatisticsStore.init();
            indexStatisticsStore.start();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        return indexStatisticsStore;
    }

    public ThrowingSupplier<CountsStore,IOException> counts()
    {
        return () ->
        {
            if ( counts == null )
            {
                counts = new GBPTreeCountsStore( pageCache, databaseLayout().countStore(), fileSystem, RecoveryCleanupWorkCollector.immediate(),
                        new CountsBuilder()
                        {
                            @Override
                            public void initialize( CountsAccessor.Updater updater, PageCursorTracer cursorTracer )
                            {
                                throw new UnsupportedOperationException( "Should not be rebuilt" );
                            }

                            @Override
                            public long lastCommittedTxId()
                            {
                                return 0;
                            }
                        }, true, PageCacheTracer.NULL, NO_MONITOR );
                counts.start( NULL );
            }
            return counts;
        };
    }

    private LabelScanStore startLabelScanStore( PageCache pageCache, IndexStoreView indexStoreView, Monitors monitors, boolean readOnly )
    {
        FullLabelStream labelStream = new FullLabelStream( indexStoreView );
        LabelScanStore labelScanStore = TokenScanStore.labelScanStore( pageCache, databaseLayout(), fileSystem, labelStream, readOnly, monitors, immediate(),
                PageCacheTracer.NULL );
        try
        {
            labelScanStore.init();
            labelScanStore.start();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return labelScanStore;
    }

    private IndexProviderMap createIndexes( PageCache pageCache, Config config, LogProvider logProvider, Monitors monitors )
    {
        LogService logService = new SimpleLogService( logProvider, logProvider );
        TokenHolders tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new ReadOnlyTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        DatabaseExtensions extensions = fixtureLife.add( instantiateExtensions( databaseLayout(), fileSystem, config, logService,
                pageCache, jobScheduler, RecoveryCleanupWorkCollector.ignore(), DatabaseInfo.COMMUNITY, monitors, tokenHolders ) );
        return fixtureLife.add( new DefaultIndexProviderMap( extensions, config ) );
    }

    public DatabaseLayout databaseLayout()
    {
        return Neo4jLayout.of( testDirectory.homeDir() ).databaseLayout( DEFAULT_DATABASE_NAME );
    }

    public Statistics getAccessStatistics()
    {
        return statistics;
    }

    public EntityUpdates nodeAsUpdates( long nodeId )
    {
        try ( StorageNodeCursor nodeCursor = storeReader.allocateNodeCursor( NULL );
              StoragePropertyCursor propertyCursor = storeReader.allocatePropertyCursor( NULL ) )
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
        tokenHolders.setInitialTokens( allReadableTokens( directStoreAccess().nativeStores().getRawNeoStores() ), NULL );
        return tokenHolders;
    }

    private TokenCreator buildTokenCreator( TokenChange propChange )
    {
        return ( name, internal ) ->
        {
            MutableInt keyId = new MutableInt();
            applyTransaction( new Transaction()
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
        public long schema()
        {
            return schemaId++;
        }

        public long node()
        {
            return nodeId++;
        }

        public int label()
        {
            return labelId++;
        }

        public long nodeLabel()
        {
            return nodeLabelsId++;
        }

        public long relationship()
        {
            return relId++;
        }

        public long relationshipGroup()
        {
            return relGroupId++;
        }

        public long property()
        {
            return propId++;
        }

        public long stringProperty()
        {
            return stringPropId++;
        }

        public long arrayProperty()
        {
            return arrayPropId++;
        }

        public int relationshipType()
        {
            return relTypeId++;
        }

        public int propertyKey()
        {
            return propKeyId++;
        }

        void updateCorrespondingIdGenerators( NeoStores neoStores )
        {
            neoStores.getNodeStore().setHighestPossibleIdInUse( nodeId );
            neoStores.getRelationshipStore().setHighestPossibleIdInUse( relId );
            neoStores.getRelationshipGroupStore().setHighestPossibleIdInUse( relGroupId );
        }
    }

    public static final class TransactionDataBuilder
    {
        private final TransactionWriter writer;
        private final NodeStore nodes;
        private final IndexingService indexingService;
        private final TokenHolders tokenHolders;
        private final AtomicInteger propKeyDynIds = new AtomicInteger( 1 );
        private final AtomicInteger labelDynIds = new AtomicInteger( 1 );
        private final AtomicInteger relTypeDynIds = new AtomicInteger( 1 );

        TransactionDataBuilder( TransactionWriter writer, NeoStores neoStores, IdGenerator next, IndexingService indexingService )
        {
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

        private int[] dynIds( int externalBase, AtomicInteger idGenerator, String name )
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

        public void propertyKey( int id, String key, boolean internal )
        {
            writer.propertyKey( id, key, internal, dynIds( id, propKeyDynIds, key ) );
            tokenHolders.propertyKeyTokens().addToken( new NamedToken( key, id ) );
        }

        public void nodeLabel( int id, String name, boolean internal )
        {
            writer.label( id, name, internal, dynIds( id, labelDynIds, name ) );
            tokenHolders.labelTokens().addToken( new NamedToken( name, id ) );
        }

        public void relationshipType( int id, String relationshipType, boolean internal )
        {
            writer.relationshipType( id, relationshipType, internal, dynIds( id, relTypeDynIds, relationshipType ) );
            tokenHolders.relationshipTypeTokens().addToken( new NamedToken( relationshipType, id ) );
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

    void stop() throws IOException
    {
        if ( directStoreAccess != null )
        {
            storeLife.shutdown();
            storeReader.close();
            neoStore.close();
            labelScanStore.shutdown();
            indexStatisticsStore.shutdown();
            jobScheduler.shutdown();
            directStoreAccess = null;
            if ( counts != null )
            {
                counts.close();
                counts = null;
            }
        }
    }

    public class Applier implements AutoCloseable
    {
        private final GraphDatabaseAPI database;
        private final TransactionRepresentationCommitProcess commitProcess;
        private final TransactionIdStore transactionIdStore;
        private final NeoStores neoStores;
        private final DatabaseManagementService managementService;
        private final IndexingService indexingService;

        Applier()
        {
            managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                    .setConfig( GraphDatabaseSettings.consistency_check_on_apply, false )
                    .setConfig( getConfig() )
                    .build();
            database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            DependencyResolver dependencyResolver = database.getDependencyResolver();

            commitProcess = new TransactionRepresentationCommitProcess(
                    dependencyResolver.resolveDependency( TransactionAppender.class ),
                    dependencyResolver.resolveDependency( StorageEngine.class ) );
            transactionIdStore = database.getDependencyResolver().resolveDependency(
                    TransactionIdStore.class );

            neoStores = dependencyResolver.resolveDependency( RecordStorageEngine.class )
                    .testAccessNeoStores();
            indexingService = dependencyResolver.resolveDependency( IndexingService.class );
        }

        public void apply( Transaction transaction ) throws KernelException
        {
            TransactionRepresentation representation =
                    transaction.representation( idGenerator(), transactionIdStore.getLastCommittedTransactionId(), neoStores, indexingService );
            commitProcess.commit( new TransactionToApply( representation, NULL ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
        }

        @Override
        public void close()
        {
            managementService.shutdown();
        }
    }

    public Applier createApplier()
    {
        return new Applier();
    }

    private void applyTransaction( Transaction transaction ) throws KernelException
    {
        // TODO you know... we could have just appended the transaction representation to the log
        // and the next startup of the store would do recovery where the transaction would have been
        // applied and all would have been well.

        try ( Applier applier = createApplier() )
        {
            applier.apply( transaction );
        }
    }

    private void generateInitialData()
    {
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() );
        DatabaseManagementService managementService = builder
                .setConfig( GraphDatabaseSettings.record_format, formatName )
                // Some tests using this fixture were written when the label_block_size was 60 and so hardcoded
                // tests and records around that. Those tests could change, but the simpler option is to just
                // keep the block size to 60 and let them be.
                .setConfig( GraphDatabaseSettings.label_block_size, 60 )
                .setConfig( GraphDatabaseSettings.consistency_check_on_apply, false )
                .setConfig( getConfig() ).build();
        // Some tests using this fixture were written when the label_block_size was 60 and so hardcoded
        // tests and records around that. Those tests could change, but the simpler option is to just
        // keep the block size to 60 and let them be.
        GraphDatabaseAPI graphDb = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            generateInitialData( graphDb );
            RecordStorageEngine storageEngine = graphDb.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
            StoreAccess stores = new StoreAccess( storageEngine.testAccessNeoStores() ).initialize();
            schemaId = stores.getSchemaStore().getHighId();
            nodeId = stores.getNodeStore().getHighId();
            labelId = (int) stores.getLabelTokenStore().getHighId();
            nodeLabelsId = stores.getNodeDynamicLabelStore().getHighId();
            relId = stores.getRelationshipStore().getHighId();
            relGroupId = stores.getRelationshipGroupStore().getHighId();
            propId = (int) stores.getPropertyStore().getHighId();
            stringPropId = stores.getStringStore().getHighId();
            arrayPropId = stores.getArrayStore().getHighId();
            relTypeId = (int) stores.getRelationshipTypeTokenStore().getHighId();
            propKeyId = (int) stores.getPropertyKeyNameStore().getHighId();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    protected abstract Map<Setting<?>, Object> getConfig();
}
