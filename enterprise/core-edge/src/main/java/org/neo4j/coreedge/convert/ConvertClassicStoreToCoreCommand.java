/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.convert;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.neo4j.coreedge.core.state.machines.id.IdAllocationState;
import org.neo4j.coreedge.core.state.machines.tx.LogIndexTxHeaderEncoding;
import org.neo4j.coreedge.core.state.storage.DurableStateStorageImporter;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.DatabasePanicEventGenerator;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelEventHandlers;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.coreedge.core.state.machines.CoreStateMachinesModule.ID_ALLOCATION_NAME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.RANDOM_NUMBER;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.LABEL_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_LABELS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_ARRAYS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STRINGS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_GROUP_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.SCHEMA_STORE_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.ARRAY_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.NODE_LABELS;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.SCHEMA;
import static org.neo4j.kernel.impl.store.id.IdType.STRING_BLOCK;

public class ConvertClassicStoreToCoreCommand
{
    private final ConversionVerifier conversionVerifier;

    public ConvertClassicStoreToCoreCommand( ConversionVerifier conversionVerifier )
    {
        this.conversionVerifier = conversionVerifier;
    }

    public void convert( File databaseDir, String recordFormat, String conversionId ) throws IOException, TransactionFailureException

    {
        ClusterSeed metadata = ClusterSeed.create( conversionId );
        verify( databaseDir, metadata );
        changeStoreId( databaseDir, metadata );
        appendNullTransactionLogEntryToSetRaftIndexToMinusOne( databaseDir, recordFormat );
        addIdAllocationState( databaseDir );
    }

    private void verify( File databaseDir, ClusterSeed metadata ) throws IOException
    {
        StoreMetadata storeMetadata = targetMetadata( databaseDir );
        conversionVerifier.conversionGuard( metadata, storeMetadata );
    }

    private StoreMetadata targetMetadata( File databaseDir ) throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File metadataStore = new File( databaseDir, MetaDataStore.DEFAULT_NAME );
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            StoreId before = readStoreId( metadataStore, pageCache );
            long lastTxId = MetaDataStore.getRecord( pageCache, metadataStore, LAST_TRANSACTION_ID );
            return new StoreMetadata( before, lastTxId );
        }
    }

    private static ClusterSeed changeStoreId( File storeDir, ClusterSeed conversionId ) throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File metadataStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            StoreId before = readStoreId( metadataStore, pageCache );

            long lastTxId = MetaDataStore.getRecord( pageCache, metadataStore, LAST_TRANSACTION_ID );

            long upgradeTime = conversionId.after().getUpgradeTime();
            long upgradeId = conversionId.after().getUpgradeId();
            MetaDataStore.setRecord( pageCache, metadataStore, UPGRADE_TIME, upgradeTime );
            MetaDataStore.setRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID, upgradeId );

            StoreId after = readStoreId( metadataStore, pageCache );
            return new ClusterSeed( before, after, lastTxId );
        }
    }

    private static StoreId readStoreId( File metadataStore, PageCache pageCache ) throws IOException
    {
        long creationTime = MetaDataStore.getRecord( pageCache, metadataStore, TIME );
        long randomNumber = MetaDataStore.getRecord( pageCache, metadataStore, RANDOM_NUMBER );
        long upgradeTime = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TIME );
        long upgradeId = MetaDataStore.getRecord( pageCache, metadataStore, UPGRADE_TRANSACTION_ID );
        return new StoreId( creationTime, randomNumber, upgradeTime, upgradeId );
    }

    private void appendNullTransactionLogEntryToSetRaftIndexToMinusOne( File dbDir, String recordFormat ) throws
            TransactionFailureException
    {
        GraphDatabaseBuilder builder = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat );

        GraphDatabaseAPI db = (GraphDatabaseAPI) builder.newGraphDatabase();

        TransactionCommitProcess commitProcess = db.getDependencyResolver().resolveDependency(
                TransactionCommitProcess.class );

        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
        byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
        tx.setHeader( txHeaderBytes, -1, -1, -1, -1, -1, -1 );

        commitProcess.commit( new TransactionToApply( tx ), CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );

        db.shutdown();
    }

    private void addIdAllocationState( File dbDir ) throws IOException
    {
        File clusterStateDirectory = new File( dbDir, "cluster-state" );

        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fileSystem );

        DurableStateStorageImporter<IdAllocationState> storage = new DurableStateStorageImporter<>(
                fileSystem, clusterStateDirectory, ID_ALLOCATION_NAME, new IdAllocationState.Marshal(),
                1000, () -> new DatabaseHealth( new DatabasePanicEventGenerator( new KernelEventHandlers( NullLog.getInstance() ) ),
                NullLog.getInstance() ), NullLogProvider.getInstance() );

        try ( Lifespan lifespan = new Lifespan( storage ) )
        {
            storage.persist( state( dbDir, factory ) );
        }
    }

    private IdAllocationState state( File dbDir, DefaultIdGeneratorFactory factory )
    {
        long[] highIds = new long[]{
                getHighId( dbDir, factory, NODE, NODE_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP, RELATIONSHIP_STORE_NAME ),
                getHighId( dbDir, factory, PROPERTY, PROPERTY_STORE_NAME ),
                getHighId( dbDir, factory, STRING_BLOCK, PROPERTY_STRINGS_STORE_NAME ),
                getHighId( dbDir, factory, ARRAY_BLOCK, PROPERTY_ARRAYS_STORE_NAME ),
                getHighId( dbDir, factory, PROPERTY_KEY_TOKEN, PROPERTY_KEY_TOKEN_STORE_NAME ),
                getHighId( dbDir, factory, PROPERTY_KEY_TOKEN_NAME, PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP_TYPE_TOKEN, RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP_TYPE_TOKEN_NAME, RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
                getHighId( dbDir, factory, LABEL_TOKEN, LABEL_TOKEN_STORE_NAME ),
                getHighId( dbDir, factory, LABEL_TOKEN_NAME, LABEL_TOKEN_NAMES_STORE_NAME ),
                getHighId( dbDir, factory, NEOSTORE_BLOCK, "" ),
                getHighId( dbDir, factory, SCHEMA, SCHEMA_STORE_NAME ),
                getHighId( dbDir, factory, NODE_LABELS, NODE_LABELS_STORE_NAME ),
                getHighId( dbDir, factory, RELATIONSHIP_GROUP, RELATIONSHIP_GROUP_STORE_NAME )};

        return new IdAllocationState( highIds, -1 );
    }

    private long getHighId( File coreDir, DefaultIdGeneratorFactory factory, IdType idType, String store )
    {
        IdGenerator idGenerator = factory.open( new File( coreDir, idFile( store ) ), idType, -1, Long.MAX_VALUE );
        long highId = idGenerator.getHighId();
        idGenerator.close();
        return highId;
    }

    private String idFile( String store )
    {
        return MetaDataStore.DEFAULT_NAME + store + ".id";
    }
}
