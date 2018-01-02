/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import java.io.File;
import java.io.IOException;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.ResponsePacker;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.TransactionChecksumLookup;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public class DefaultMasterImplSPI implements MasterImpl.SPI
{
    private static final int ID_GRAB_SIZE = 1000;
    static final String STORE_COPY_CHECKPOINT_TRIGGER = "store copy";

    private final GraphDatabaseAPI graphDb;
    private final TransactionChecksumLookup txChecksumLookup;
    private final FileSystemAbstraction fileSystem;
    private final LabelTokenHolder labels;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final IdGeneratorFactory idGeneratorFactory;
    private final NeoStoreDataSource neoStoreDataSource;
    private final File storeDir;
    private final ResponsePacker responsePacker;
    private final Monitors monitors;
    private final PageCache pageCache;

    private final TransactionCommitProcess transactionCommitProcess;
    private final CheckPointer checkPointer;
    private final StoreCopyCheckPointMutex mutex;

    public DefaultMasterImplSPI( final GraphDatabaseAPI graphDb,
                                 FileSystemAbstraction fileSystemAbstraction,
                                 Monitors monitors,
                                 LabelTokenHolder labels, PropertyKeyTokenHolder propertyKeyTokenHolder,
                                 RelationshipTypeTokenHolder relationshipTypeTokenHolder,
                                 IdGeneratorFactory idGeneratorFactory,
                                 TransactionCommitProcess transactionCommitProcess,
                                 CheckPointer checkPointer,
                                 TransactionIdStore transactionIdStore,
                                 LogicalTransactionStore logicalTransactionStore,
                                 NeoStoreDataSource neoStoreDataSource,
                                 PageCache pageCache,
                                 StoreCopyCheckPointMutex mutex,
                                 LogProvider logProvider )
    {
        this.graphDb = graphDb;
        this.fileSystem = fileSystemAbstraction;
        this.labels = labels;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.idGeneratorFactory = idGeneratorFactory;
        this.transactionCommitProcess = transactionCommitProcess;
        this.checkPointer = checkPointer;
        this.neoStoreDataSource = neoStoreDataSource;
        this.mutex = mutex;
        this.storeDir = graphDb.getStoreDir();
        this.txChecksumLookup = new TransactionChecksumLookup( transactionIdStore, logicalTransactionStore );
        this.responsePacker = new ResponsePacker( logicalTransactionStore, transactionIdStore, graphDb::storeId );
        this.monitors = monitors;
        this.pageCache = pageCache;
        monitors.addMonitorListener( new LoggingStoreCopyServerMonitor( logProvider.getLog( StoreCopyServer.class ) ),
                StoreCopyServer.class.getName() );
    }

    @Override
    public boolean isAccessible()
    {
        // Wait for 5s for the database to become available, if not already so
        return graphDb.isAvailable( 5000 );
    }

    @Override
    public int getOrCreateLabel( String name )
    {
        return labels.getOrCreateId( name );
    }

    @Override
    public int getOrCreateProperty( String name )
    {
        return propertyKeyTokenHolder.getOrCreateId( name );
    }

    @Override
    public IdAllocation allocateIds( IdType idType )
    {
        IdGenerator generator = idGeneratorFactory.get( idType );
        return new IdAllocation( generator.nextIdBatch( ID_GRAB_SIZE ), generator.getHighId(),
                generator.getDefragCount() );
    }

    @Override
    public StoreId storeId()
    {
        return graphDb.storeId();
    }

    @Override
    public long applyPreparedTransaction( TransactionRepresentation preparedTransaction )
            throws IOException, TransactionFailureException
    {
        return transactionCommitProcess.commit( new TransactionToApply( preparedTransaction ), CommitEvent.NULL,
                TransactionApplicationMode.EXTERNAL );
    }

    @Override
    public Integer createRelationshipType( String name )
    {
        return relationshipTypeTokenHolder.getOrCreateId( name );
    }

    @Override
    public long getTransactionChecksum( long txId ) throws IOException
    {
        return txChecksumLookup.applyAsLong( txId );
    }

    @Override
    public RequestContext flushStoresAndStreamStoreFiles( StoreWriter writer )
    {
        StoreCopyServer streamer = new StoreCopyServer( neoStoreDataSource, checkPointer, fileSystem, storeDir,
                monitors.newMonitor( StoreCopyServer.Monitor.class, StoreCopyServer.class ),
                pageCache, mutex );
        return streamer.flushStoresAndStreamStoreFiles( STORE_COPY_CHECKPOINT_TRIGGER, writer, false );
    }

    @Override
    public <T> Response<T> packTransactionStreamResponse( RequestContext context, T response )
    {
        return responsePacker.packTransactionStreamResponse( context, response );
    }

    @Override
    public <T> Response<T> packTransactionObligationResponse( RequestContext context, T response )
    {
        return responsePacker.packTransactionObligationResponse( context, response );
    }

    @Override
    public <T> Response<T> packEmptyResponse( T response )
    {
        return responsePacker.packEmptyResponse( response );
    }

    private static class LoggingStoreCopyServerMonitor implements StoreCopyServer.Monitor
    {
        private Log log;

        LoggingStoreCopyServerMonitor( Log log )
        {
            this.log = log;
        }

        @Override
        public void startTryCheckPoint( String storeCopyIdentifier )
        {
            log.debug( "%s: try to checkpoint before sending store.", storeCopyIdentifier );
        }

        @Override
        public void finishTryCheckPoint( String storeCopyIdentifier )
        {
            log.debug( "%s: checkpoint before sending store completed.", storeCopyIdentifier );
        }

        @Override
        public void startStreamingStoreFile( File file, String storeCopyIdentifier )
        {
            log.debug( "%s: start streaming file %s.", storeCopyIdentifier, file );
        }

        @Override
        public void finishStreamingStoreFile( File file, String storeCopyIdentifier )
        {
            log.debug( "%s: finish streaming file %s.", storeCopyIdentifier, file );
        }

        @Override
        public void startStreamingStoreFiles( String storeCopyIdentifier )
        {
            log.debug( "%s: start streaming store files.", storeCopyIdentifier );
        }

        @Override
        public void finishStreamingStoreFiles( String storeCopyIdentifier )
        {
            log.debug( "%s: finish streaming store files.", storeCopyIdentifier );
        }

        @Override
        public void startStreamingTransactions( long startTxId, String storeCopyIdentifier )
        {
            log.debug( "%s: start streaming transaction starting %d.", storeCopyIdentifier, startTxId );
        }

        @Override
        public void finishStreamingTransactions( long endTxId, String storeCopyIdentifier )
        {
            log.debug( "%s: finish streaming transactions at %d.", storeCopyIdentifier, endTxId );
        }
    }
}
