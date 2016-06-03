/*
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
package org.neo4j.consistency.checking;

import org.apache.commons.lang3.StringUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.Collection;

import org.neo4j.consistency.statistics.AccessStatistics;
import org.neo4j.consistency.statistics.AccessStatsKeepingStoreAccess;
import org.neo4j.consistency.statistics.DefaultCounts;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.consistency.statistics.VerboseStatistics;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.lucene.LuceneLabelScanStoreBuilder;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.storeview.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.schema.SchemaRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public abstract class GraphStoreFixture extends PageCacheRule implements TestRule
{
    private DirectStoreAccess directStoreAccess;
    private Statistics statistics;
    private final boolean keepStatistics;
    private NeoStores neoStore;
    private File directory;
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
    private String formatName = StringUtils.EMPTY;

    public GraphStoreFixture( boolean keepStatistics, String formatName )
    {
        this.keepStatistics = keepStatistics;
        this.formatName = formatName;
    }

    public GraphStoreFixture( String formatName )
    {
        this( false, formatName );
    }

    public GraphStoreFixture()
    {
        this( false, StandardV3_0.NAME );
    }

    public void apply( Transaction transaction ) throws TransactionFailureException
    {
        applyTransaction( transaction );
    }

    public DirectStoreAccess directStoreAccess()
    {
        if ( directStoreAccess == null )
        {
            DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            PageCache pageCache = getPageCache( fileSystem );
            Config config = new Config( stringMap( GraphDatabaseSettings.record_format.name(), formatName ) );
            neoStore = new StoreFactory( fileSystem, directory, pageCache,
                    RecordFormatSelector.autoSelectFormat(config,  NullLogService.getInstance() ),
                    NullLogProvider.getInstance() ).openAllNeoStores();
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

            IndexStoreView indexStoreView = new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE, nativeStores.getRawNeoStores() );
            LabelScanStore labelScanStore = new LuceneLabelScanStoreBuilder( directory, indexStoreView,
                    fileSystem, FormattedLogProvider.toOutputStream( System.out ) )
                    .build();
            directStoreAccess = new DirectStoreAccess( nativeStores, labelScanStore, createIndexes( fileSystem ) );
        }
        return directStoreAccess;
    }

    private SchemaIndexProvider createIndexes( FileSystemAbstraction fileSystem )
    {
        return new LuceneSchemaIndexProvider( fileSystem, DirectoryFactory.PERSISTENT, directory );
    }

    public File directory()
    {
        return directory;
    }

    public Statistics getAccessStatistics()
    {
        return statistics;
    }

    public abstract static class Transaction
    {
        public final long startTimestamp = currentTimeMillis();

        protected abstract void transactionData( TransactionDataBuilder tx, IdGenerator next );

        public TransactionRepresentation representation( IdGenerator idGenerator, int masterId, int authorId,
                                                         long lastCommittedTx, NeoStores neoStores )
        {
            TransactionWriter writer = new TransactionWriter( neoStores );
            transactionData( new TransactionDataBuilder( writer, neoStores.getNodeStore() ), idGenerator );
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

        public void updateCorrespondingIdGenerators( NeoStores neoStores )
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

        public TransactionDataBuilder( TransactionWriter writer, NodeStore nodes )
        {
            this.writer = writer;
            this.nodes = nodes;
        }

        public void createSchema( Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords,
                SchemaRule rule )
        {
            writer.createSchema( beforeRecords, afterRecords, rule );
        }

        // In the following three methods there's an assumption that all tokens use one dynamic record
        // and since the first record in a dynamic store the id starts at 1 instead of 0... hence the +1

        public void propertyKey( int id, String key )
        {
            writer.propertyKey( id, key, id + 1 );
        }

        public void nodeLabel( int id, String name )
        {
            writer.label( id, name, id + 1 );
        }

        public void relationshipType( int id, String relationshipType )
        {
            writer.relationshipType( id, relationshipType, id + 1 );
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
            writer.incrementNodeCount( ReadOperations.ANY_LABEL, delta );
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

    protected void start( @SuppressWarnings("UnusedParameters") File storeDir )
    {
        // allow for override
    }

    protected void stop() throws Throwable
    {
        if ( directStoreAccess != null )
        {
            neoStore.close();
            directStoreAccess.close();
            directStoreAccess = null;
        }
    }

    protected int myId()
    {
        return 1;
    }

    protected int masterId()
    {
        return -1;
    }

    @SuppressWarnings("deprecation")
    protected void applyTransaction( Transaction transaction ) throws TransactionFailureException
    {
        // TODO you know... we could have just appended the transaction representation to the log
        // and the next startup of the store would do recovery where the transaction would have been
        // applied and all would have been well.

        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( directory )
                             .setConfig( GraphDatabaseSettings.record_format, formatName );
        GraphDatabaseAPI database = (GraphDatabaseAPI) builder.newGraphDatabase();
        try
        {
            DependencyResolver dependencyResolver = database.getDependencyResolver();

            TransactionRepresentationCommitProcess commitProcess =
                    new TransactionRepresentationCommitProcess(
                            dependencyResolver.resolveDependency( TransactionAppender.class ),
                            dependencyResolver.resolveDependency( StorageEngine.class ) );
            TransactionIdStore transactionIdStore = database.getDependencyResolver().resolveDependency(
                    TransactionIdStore.class );

            NeoStores neoStores = database.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                    .testAccessNeoStores();

            TransactionRepresentation representation = transaction.representation( idGenerator(), masterId(), myId(),
                    transactionIdStore.getLastCommittedTransactionId(), neoStores );
            commitProcess.commit( new TransactionToApply( representation ), CommitEvent.NULL,
                    TransactionApplicationMode.EXTERNAL );
        }
        finally
        {
            database.shutdown();
        }
    }

    private void generateInitialData()
    {
        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( directory );
        GraphDatabaseAPI graphDb = (GraphDatabaseAPI) builder
                .setConfig( GraphDatabaseSettings.record_format, formatName )
                // Some tests using this fixture were written when the label_block_size was 60 and so hardcoded
                // tests and records around that. Those tests could change, but the simpler option is to just
                // keep the block size to 60 and let them be.
                .setConfig( GraphDatabaseSettings.label_block_size, "60" )
                .newGraphDatabase();
        try
        {
            generateInitialData( graphDb );
            StoreAccess stores = new StoreAccess( graphDb.getDependencyResolver()
                    .resolveDependency( RecordStorageEngine.class ).testAccessNeoStores() ).initialize();
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
        final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( description.getTestClass() );
        return super.apply( directory.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                GraphStoreFixture.this.directory = directory.graphDbDir();
                try
                {
                    generateInitialData();
                    start( GraphStoreFixture.this.directory );
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
