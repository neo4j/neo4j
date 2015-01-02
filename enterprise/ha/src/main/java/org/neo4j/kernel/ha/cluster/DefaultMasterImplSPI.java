/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.StoreWriter;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.com.master.MasterImpl;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.BackupMonitor;
import org.neo4j.kernel.monitoring.Monitors;

class DefaultMasterImplSPI implements MasterImpl.SPI
{
    private static final int ID_GRAB_SIZE = 1000;
    private final GraphDatabaseAPI graphDb;
    private final Logging logging;
    private final TransactionManager txManager;
    private Monitors monitors;

    public DefaultMasterImplSPI( GraphDatabaseAPI graphDb, Logging logging,
                                 TransactionManager txManager, Monitors monitors )
    {
        this.graphDb = graphDb;
        this.logging = logging;
        this.txManager = txManager;
        this.monitors = monitors;
    }

    @Override
    public boolean isAccessible()
    {
        // Wait for 5s for the database to become available, if not already so
        return graphDb.isAvailable( 5000 );
    }

    @Override
    public void acquireLock( MasterImpl.LockGrabber grabber, Object... entities )
    {
        LockManager lockManager = graphDb.getLockManager();
        TransactionState state = ((AbstractTransactionManager) graphDb.getTxManager()).getTransactionState();
        for ( Object entity : entities )
        {
            grabber.grab( lockManager, state, entity );
        }
    }

    @Override
    public Transaction beginTx() throws SystemException, NotSupportedException
    {
        txManager.begin();
        return txManager.getTransaction();
    }

    @Override
    public void finishTransaction( boolean success )
    {
        try
        {
            if ( success )
            {
                txManager.commit();
            }
            else
            {
                txManager.rollback();
            }
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
    }

    @Override
    public void suspendTransaction() throws SystemException
    {
        txManager.suspend();
    }

    @Override
    public void resumeTransaction( Transaction transaction )
    {
        try
        {
            txManager.resume( transaction );
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( e );
        }
    }

    @Override
    public GraphProperties graphProperties()
    {
        return graphDb.getDependencyResolver().resolveDependency( NodeManager.class ).getGraphProperties();
    }

    @Override
    public IdAllocation allocateIds( IdType idType )
    {
        IdGenerator generator = graphDb.getIdGeneratorFactory().get( idType );
        return new IdAllocation( generator.nextIdBatch( ID_GRAB_SIZE ), generator.getHighId(),
                generator.getDefragCount() );
    }

    @Override
    public StoreId storeId()
    {
        return graphDb.getStoreId();
    }

    @Override
    public long applyPreparedTransaction( String resource, ReadableByteChannel preparedTransaction ) throws IOException
    {
        XaDataSource dataSource = graphDb.getXaDataSourceManager().getXaDataSource( resource );
        return dataSource.applyPreparedTransaction( preparedTransaction );
    }

    @Override
    public Integer createRelationshipType( String name )
    {
        graphDb.getRelationshipTypeHolder().addValidRelationshipType( name, true );
        return graphDb.getRelationshipTypeHolder().getIdFor( name );
    }

    @Override
    public Pair<Integer, Long> getMasterIdForCommittedTx( long txId ) throws IOException
    {
        XaDataSource nioneoDataSource = graphDb.getXaDataSourceManager().getNeoStoreDataSource();
        return nioneoDataSource.getMasterForCommittedTx( txId );
    }

    @Override
    public RequestContext rotateLogsAndStreamStoreFiles( StoreWriter writer )
    {
        return ServerUtil.rotateLogsAndStreamStoreFiles( graphDb.getStoreDir(), graphDb.getXaDataSourceManager(),
                graphDb.getKernelPanicGenerator(), logging.getMessagesLog( MasterImpl.class ), true, writer,
                monitors.newMonitor( BackupMonitor.class, getClass() ) );
    }

    @Override
    public Response<Void> copyTransactions( String dsName, long startTxId, long endTxId )
    {
        return ServerUtil.getTransactions( graphDb, dsName, startTxId, endTxId );
    }

    @Override
    public <T> Response<T> packResponse( RequestContext context, T response, Predicate<Long> filter )
    {
        return ServerUtil.packResponse( graphDb.getStoreId(), graphDb.getXaDataSourceManager(), context, response,
                filter );
    }

    @Override
    public void pushTransaction( String resourceName, int eventIdentifier, long tx, int machineId )
    {
        graphDb.getTxIdGenerator().committed( graphDb.getXaDataSourceManager().getXaDataSource( resourceName ),
                eventIdentifier, tx, machineId );
    }
}
