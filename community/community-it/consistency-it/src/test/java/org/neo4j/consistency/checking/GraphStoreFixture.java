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
package org.neo4j.consistency.checking;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.scan.FullLabelStream;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.ReadOnlyTokenCreator;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
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
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.ConfigurablePageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.consistency.internal.SchemaIndexExtensionLoader.instantiateExtensions;

public abstract class GraphStoreFixture extends ConfigurablePageCacheRule implements TestRule
{
    private DirectStoreAccess directStoreAccess;
    private Statistics statistics;
    private final boolean keepStatistics;
    private NeoStores neoStore;
    private StorageReader storeReader;
    private TestDirectory directory;
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

    private GraphStoreFixture( boolean keepStatistics, String formatName )
    {
        this.keepStatistics = keepStatistics;
        this.formatName = formatName;
    }

    protected GraphStoreFixture( String formatName )
    {
        this( false, formatName );
    }

    @Override
    protected void after( boolean success )
    {
        storeLife.shutdown();
        fixtureLife.shutdown();
        super.after( success );
        if ( fileSystem != null )
        {
            try
            {
                fileSystem.close();
            }
            catch ( IOException e )
            {
                throw new AssertionError( "Failed to stop file system after test", e );
            }
        }
    }

    public void apply( Transaction transaction ) throws TransactionFailureException
    {
        applyTransaction( transaction );
    }

    public DirectStoreAccess directStoreAccess()
    {
        if ( directStoreAccess == null )
        {
            fileSystem = new DefaultFileSystemAbstraction();
            PageCache pageCache = getPageCache( fileSystem );
            LogProvider logProvider = NullLogProvider.getInstance();
            Config config = Config.defaults();
            DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem );
            StoreFactory storeFactory = new StoreFactory(
                    directory.databaseLayout(), config, idGeneratorFactory, pageCache, fileSystem, logProvider, EmptyVersionContextSupplier.EMPTY );
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

            CountsTracker counts = new CountsTracker( logProvider, fileSystem, pageCache, config, databaseLayout(), EmptyVersionContextSupplier.EMPTY );
            storeLife.add( counts );

            IndexStoreView indexStoreView = new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE,
                    () -> new RecordStorageReader( nativeStores.getRawNeoStores() ) );

            Monitors monitors = new Monitors();
            LabelScanStore labelScanStore = startLabelScanStore( pageCache, indexStoreView, monitors );
            IndexProviderMap indexes = createIndexes( pageCache, config, logProvider, monitors);
            directStoreAccess = new DirectStoreAccess( nativeStores, labelScanStore, indexes, counts );
            storeReader = new RecordStorageReader( neoStore );
        }
        return directStoreAccess;
    }

    private LabelScanStore startLabelScanStore( PageCache pageCache, IndexStoreView indexStoreView, Monitors monitors )
    {
        NativeLabelScanStore labelScanStore =
                new NativeLabelScanStore( pageCache, databaseLayout(), fileSystem, new FullLabelStream( indexStoreView ), false, monitors,
                        RecoveryCleanupWorkCollector.immediate() );
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
        return directory.databaseLayout();
    }

    public Statistics getAccessStatistics()
    {
        return statistics;
    }

    public EntityUpdates nodeAsUpdates( long nodeId )
    {
        try ( StorageNodeCursor nodeCursor = storeReader.allocateNodeCursor();
              StoragePropertyCursor propertyCursor = storeReader.allocatePropertyCursor() )
        {
            nodeCursor.single( nodeId );
            long[] labels;
            if ( !nodeCursor.next() || !nodeCursor.hasProperties() || (labels = nodeCursor.labels()).length == 0 )
            {
                return null;
            }
            nodeCursor.properties( propertyCursor );
            EntityUpdates.Builder update = EntityUpdates.forEntity( nodeId ).withTokens( labels );
            while ( propertyCursor.next() )
            {
                update.added( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
            }
            return update.build();
        }
    }

    interface TokenChange
    {
        int createToken( String name, TransactionDataBuilder tx, IdGenerator next );
    }

    public TokenHolders writableTokenHolders()
    {
        TokenHolder propertyKeyTokens = new DelegatingTokenHolder( buildTokenCreator( ( name, tx, next ) ->
        {
            int id = next.propertyKey();
            tx.propertyKey( id, name );
            return id;
        } ), TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolder labelTokens = new DelegatingTokenHolder( buildTokenCreator( ( name, tx, next ) ->
        {
            int id = next.label();
            tx.nodeLabel( id, name );
            return id;
        } ), TokenHolder.TYPE_LABEL );
        TokenHolder relationshipTypeTokens = new DelegatingTokenHolder( buildTokenCreator( ( name, tx, next ) ->
        {
            int id = next.relationshipType();
            tx.relationshipType( id, name );
            return id;
        } ), TokenHolder.TYPE_RELATIONSHIP_TYPE );
        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokens, labelTokens, relationshipTypeTokens );
        RecordStorageEngineFactory.setInitialTokens( tokenHolders, directStoreAccess().nativeStores().getRawNeoStores() );
        return tokenHolders;
    }

    private TokenCreator buildTokenCreator( TokenChange propChange )
    {
        return name ->
        {
            MutableInt keyId = new MutableInt();
            applyTransaction( new Transaction()
            {
                @Override
                protected void transactionData( TransactionDataBuilder tx, IdGenerator next )
                {
                    keyId.setValue( propChange.createToken( name, tx, next ) );
                }
            } );
            return keyId.intValue();
        };
    }

    public abstract static class Transaction
    {
        final long startTimestamp = currentTimeMillis();

        protected abstract void transactionData( TransactionDataBuilder tx, IdGenerator next );

        public TransactionRepresentation representation( IdGenerator idGenerator, int masterId, int authorId,
                                                         long lastCommittedTx, NeoStores neoStores )
        {
            TransactionWriter writer = new TransactionWriter( neoStores );
            transactionData( new TransactionDataBuilder( writer, neoStores, idGenerator ), idGenerator );
            idGenerator.updateCorrespondingIdGenerators( neoStores );
            return writer.representation( new byte[0], masterId, authorId, startTimestamp, lastCommittedTx,
                   currentTimeMillis() );
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
        private final TokenHolders tokenHolders;
        private final AtomicInteger propKeyDynIds = new AtomicInteger( 1 );
        private final AtomicInteger labelDynIds = new AtomicInteger( 1 );
        private final AtomicInteger relTypeDynIds = new AtomicInteger( 1 );

        TransactionDataBuilder( TransactionWriter writer, NeoStores neoStores, IdGenerator next )
        {
            this.writer = writer;
            this.nodes = neoStores.getNodeStore();

            TokenHolder propTokens = new DelegatingTokenHolder( name ->
            {
                int id = next.propertyKey();
                writer.propertyKey( id, name, dynIds( 0, propKeyDynIds, name ) );
                return id;
            }, TokenHolder.TYPE_PROPERTY_KEY );

            TokenHolder labelTokens = new DelegatingTokenHolder( name ->
            {
                int id = next.label();
                writer.label( id, name, dynIds( 0, labelDynIds, name ) );
                return id;
            }, TokenHolder.TYPE_LABEL );

            TokenHolder relTypeTokens = new DelegatingTokenHolder( name ->
            {
                int id = next.relationshipType();
                writer.relationshipType( id, name, dynIds( 0, relTypeDynIds, name ) );
                return id;
            }, TokenHolder.TYPE_RELATIONSHIP_TYPE );

            this.tokenHolders = new TokenHolders( propTokens, labelTokens, relTypeTokens );
            RecordStorageEngineFactory.setInitialTokens( tokenHolders, neoStores );
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

        public void propertyKey( int id, String key )
        {
            writer.propertyKey( id, key, dynIds( id, propKeyDynIds, key ) );
            tokenHolders.propertyKeyTokens().addToken( new NamedToken( key, id ) );
        }

        public void nodeLabel( int id, String name )
        {
            writer.label( id, name, dynIds( id, labelDynIds, name ) );
            tokenHolders.labelTokens().addToken( new NamedToken( name, id ) );
        }

        public void relationshipType( int id, String relationshipType )
        {
            writer.relationshipType( id, relationshipType, dynIds( id, relTypeDynIds, relationshipType ) );
            tokenHolders.relationshipTypeTokens().addToken( new NamedToken( relationshipType, id ) );
        }

        public void update( NeoStoreRecord before, NeoStoreRecord after )
        {
            writer.update( before, after );
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

        public void update(  RelationshipGroupRecord before, RelationshipGroupRecord after )
        {
            writer.update( before, after );
        }

        public void delete(  RelationshipGroupRecord group )
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
            writer.incrementNodeCount( StatementConstants.ANY_LABEL, delta );
            for ( long label : NodeLabelsField.parseLabelsField( node ).get( nodes ) )
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
    }

    protected abstract void generateInitialData( GraphDatabaseService graphDb );

    protected void start( @SuppressWarnings( "UnusedParameters" ) File storeDir )
    {
        // allow for override
    }

    protected void stop() throws Throwable
    {
        if ( directStoreAccess != null )
        {
            storeLife.shutdown();
            storeReader.close();
            neoStore.close();
            directStoreAccess.close();
            directStoreAccess = null;
        }
    }

    private int myId()
    {
        return 1;
    }

    private int masterId()
    {
        return -1;
    }

    public class Applier implements AutoCloseable
    {
        private final GraphDatabaseAPI database;
        private final TransactionRepresentationCommitProcess commitProcess;
        private final TransactionIdStore transactionIdStore;
        private final NeoStores neoStores;

        Applier()
        {
            database = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( directory.databaseDir() )
                    .setConfig( "dbms.backup.enabled", "false" )
                    .newGraphDatabase();
            DependencyResolver dependencyResolver = database.getDependencyResolver();

            commitProcess = new TransactionRepresentationCommitProcess(
                    dependencyResolver.resolveDependency( TransactionAppender.class ),
                    dependencyResolver.resolveDependency( StorageEngine.class ) );
            transactionIdStore = database.getDependencyResolver().resolveDependency(
                    TransactionIdStore.class );

            neoStores = database.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                    .testAccessNeoStores();
        }

        public void apply( Transaction transaction ) throws TransactionFailureException
        {
            TransactionRepresentation representation = transaction.representation( idGenerator(), masterId(), myId(),
                    transactionIdStore.getLastCommittedTransactionId(), neoStores );
            commitProcess.commit( new TransactionToApply( representation ), CommitEvent.NULL,
                    TransactionApplicationMode.EXTERNAL );
        }

        @Override
        public void close()
        {
            database.shutdown();
        }
    }

    public Applier createApplier()
    {
        return new Applier();
    }

    private void applyTransaction( Transaction transaction ) throws TransactionFailureException
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
        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( directory.databaseDir() );
        GraphDatabaseAPI graphDb = (GraphDatabaseAPI) builder
                .setConfig( GraphDatabaseSettings.record_format, formatName )
                // Some tests using this fixture were written when the label_block_size was 60 and so hardcoded
                // tests and records around that. Those tests could change, but the simpler option is to just
                // keep the block size to 60 and let them be.
                .setConfig( GraphDatabaseSettings.label_block_size, "60" )
                .setConfig( "dbms.backup.enabled", "false" )
                .newGraphDatabase();
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
            graphDb.shutdown();
        }
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        final TestDirectory directory = TestDirectory.testDirectory( description.getTestClass() );
        return directory.apply( super.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                GraphStoreFixture.this.directory = directory;
                try
                {
                    generateInitialData();
                    start( GraphStoreFixture.this.directory.databaseDir() );
                    try
                    {
                        base.evaluate();
                    }
                    finally
                    {
                        stop();
                    }
                }
                finally
                {
                    GraphStoreFixture.this.directory = null;
                }
            }
        }, description ), description );
    }
}
