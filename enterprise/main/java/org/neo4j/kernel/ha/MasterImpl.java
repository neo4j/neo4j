/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

/**
 * This is the real master code that executes on a master. The actual
 * communication over network happens in {@link MasterClient} and
 * {@link MasterServer}.
 */
public class MasterImpl implements Master
{
    private static final int ID_GRAB_SIZE = 1000;

    private static final Predicate<Long> ALL = new Predicate<Long>()
    {
        public boolean accept( Long item )
        {
            return true;
        }
    };

    private final GraphDatabaseService graphDb;
    private final Config graphDbConfig;

    private final Map<SlaveContext, Transaction> transactions =
            Collections.synchronizedMap( new HashMap<SlaveContext, Transaction>() );

    public MasterImpl( GraphDatabaseService db )
    {
        this.graphDb = db;
        this.graphDbConfig = ((AbstractGraphDatabase) db).getConfig();
    }

    public GraphDatabaseService getGraphDb()
    {
        return this.graphDb;
    }

    private <T extends PropertyContainer> Response<LockResult> acquireLock( SlaveContext context,
            LockGrabber lockGrabber, T... entities )
    {
        Transaction otherTx = suspendOtherAndResumeThis( context );
        try
        {
            LockManager lockManager = graphDbConfig.getLockManager();
            LockReleaser lockReleaser = graphDbConfig.getLockReleaser();
            for ( T entity : entities )
            {
                lockGrabber.grab( lockManager, lockReleaser, entity );
            }
            return packResponse( context, new LockResult( LockStatus.OK_LOCKED ), ALL );
        }
        catch ( DeadlockDetectedException e )
        {
            return packResponse( context, new LockResult( e.getMessage() ), ALL );
        }
        catch ( IllegalResourceException e )
        {
            return packResponse( context, new LockResult( LockStatus.NOT_LOCKED ), ALL );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx, context );
        }
    }

    private Transaction getTx( SlaveContext txId )
    {
        return transactions.get( txId );
    }

    private Transaction beginTx( SlaveContext txId )
    {
        try
        {
            TransactionManager txManager = graphDbConfig.getTxModule().getTxManager();
            txManager.begin();
            Transaction tx = txManager.getTransaction();
            transactions.put( txId, tx );
            return tx;
        }
        catch ( NotSupportedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }

    Transaction suspendOtherAndResumeThis( SlaveContext txId )
    {
        try
        {
            TransactionManager txManager = graphDbConfig.getTxModule().getTxManager();
            Transaction otherTx = txManager.getTransaction();
            Transaction transaction = getTx( txId );
            if ( otherTx != null && otherTx == transaction )
            {
                return null;
            }
            else
            {
                if ( otherTx != null )
                {
                    txManager.suspend();
                }
                if ( transaction == null )
                {
                    beginTx( txId );
                }
                else
                {
                    txManager.resume( transaction );
                }
                return otherTx;
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

    void suspendThisAndResumeOther( Transaction otherTx, SlaveContext txId )
    {
        try
        {
            TransactionManager txManager = graphDbConfig.getTxModule().getTxManager();
            txManager.suspend();
            if ( otherTx != null )
            {
                txManager.resume( otherTx );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

    void rollbackThisAndResumeOther( Transaction otherTx, SlaveContext txId )
    {
        try
        {
            TransactionManager txManager = graphDbConfig.getTxModule().getTxManager();
            txManager.rollback();
            transactions.remove( txId );
            if ( otherTx != null )
            {
                txManager.resume( otherTx );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

    public Response<LockResult> acquireNodeReadLock( SlaveContext context, long... nodes )
    {
        return acquireLock( context, READ_LOCK_GRABBER, nodesById( nodes ) );
    }

    public Response<LockResult> acquireNodeWriteLock( SlaveContext context, long... nodes )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, nodesById(nodes) );
    }

    public Response<LockResult> acquireRelationshipReadLock( SlaveContext context,
            long... relationships )
    {
        return acquireLock( context, READ_LOCK_GRABBER, relationshipsById(relationships) );
    }

    public Response<LockResult> acquireRelationshipWriteLock( SlaveContext context,
            long... relationships )
    {
        return acquireLock( context, WRITE_LOCK_GRABBER, relationshipsById(relationships) );
    }

    private Node[] nodesById( long[] ids )
    {
        Node[] result = new Node[ids.length];
        for ( int i = 0; i < ids.length; i++ )
        {
            result[i] = new LockableNode( (int) ids[i] );
        }
        return result;
    }

    private Relationship[] relationshipsById( long[] ids )
    {
        Relationship[] result = new Relationship[ids.length];
        for ( int i = 0; i < ids.length; i++ )
        {
            result[i] = new LockableRelationship( (int) ids[i] );
        }
        return result;
    }

    public IdAllocation allocateIds( IdType idType )
    {
        IdGenerator generator = graphDbConfig.getIdGeneratorFactory().get( idType );
        return new IdAllocation( generator.nextIdBatch( ID_GRAB_SIZE ),
                generator.getHighId(), generator.getDefragCount() );
    }

    public Response<Long> commitSingleResourceTransaction( SlaveContext context,
            String resource, TxExtractor txGetter )
    {
        Transaction otherTx = suspendOtherAndResumeThis( context );
        try
        {
            XaDataSource dataSource = graphDbConfig.getTxModule()
                    .getXaDataSourceManager().getXaDataSource( resource );
            final long txId = dataSource.applyPreparedTransaction( txGetter.extract() );
            Predicate<Long> notThisTx = new Predicate<Long>()
            {
                public boolean accept( Long item )
                {
                    return item != txId;
                }
            };
            return packResponse( context, txId, notThisTx );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            return new FailedResponse<Long>();
        }
        finally
        {
            suspendThisAndResumeOther( otherTx, context );
        }
    }

    public Response<Void> finishTransaction( SlaveContext context )
    {
        Transaction otherTx = suspendOtherAndResumeThis( context );
        rollbackThisAndResumeOther( otherTx, context );
        return packResponse( context, null, ALL );
    }

    public Response<Integer> createRelationshipType( SlaveContext context, String name )
    {
        // Does this type exist already?
        Integer id = graphDbConfig.getRelationshipTypeHolder().getIdFor( name );
        if ( id != null )
        {
            // OK, return
            return packResponse( context, id, ALL );
        }

        // No? Create it then
        id = graphDbConfig.getRelationshipTypeCreator().getOrCreate(
                graphDbConfig.getTxModule().getTxManager(),
                graphDbConfig.getIdGeneratorModule().getIdGenerator(),
                graphDbConfig.getPersistenceModule().getPersistenceManager(),
                graphDbConfig.getRelationshipTypeHolder(), name );
        return packResponse( context, id, ALL );
    }

    private <T> Response<T> packResponse( SlaveContext context, T response, Predicate<Long> filter )
    {
        List<Triplet<String, Long, TxExtractor>> stream = new ArrayList<Triplet<String, Long, TxExtractor>>();
        Set<String> resourceNames = new HashSet<String>();
        for ( Pair<String, Long> txEntry : context.lastAppliedTransactions() )
        {
            String resourceName = txEntry.first();
            final XaDataSource dataSource = graphDbConfig.getTxModule().getXaDataSourceManager().getXaDataSource(
                    resourceName );
            if ( dataSource == null )
            {
                throw new RuntimeException( "No data source '" + resourceName + "' found" );
            }
            resourceNames.add( resourceName );
            long masterLastTx = dataSource.getLastCommittedTxId();
            for ( long txId = txEntry.other() + 1; txId <= masterLastTx; txId++ )
            {
                if ( filter.accept( txId ) )
                {
                    final long tx = txId;
                    TxExtractor extractor = new TxExtractor()
                    {
                        @Override
                        public ReadableByteChannel extract()
                        {
                            try
                            {
                                return dataSource.getCommittedTransaction( tx );
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }

                        @Override
                        public void extract( LogBuffer buffer )
                        {
                            try
                            {
                                dataSource.getCommittedTransaction( tx, buffer );
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }
                    };
                    stream.add( Triplet.of( resourceName, txId, extractor ) );
                }
            }
        }
        return new Response<T>( response, TransactionStream.create( resourceNames, stream ) );
    }

    public Response<Void> pullUpdates( SlaveContext context )
    {
        return packResponse( context, null, ALL );
    }

    public int getMasterIdForCommittedTx( long txId )
    {
        try
        {
            XaDataSource nioneoDataSource = graphDbConfig.getTxModule()
                    .getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
            return nioneoDataSource.getMasterForCommittedTx( txId );
        }
        catch ( IOException e )
        {
            return XaLogicalLog.MASTER_ID_REPRESENTING_NO_MASTER;
        }
    }

    public Response<Void> copyStore( SlaveContext context, StoreWriter writer )
    {
        Collection<XaDataSource> sources = graphDbConfig.getTxModule().getXaDataSourceManager().getAllRegisteredDataSources();
        Pair<String, Long>[] appliedTransactions = new Pair[sources.size()];
        int i = 0;
        for ( XaDataSource ds : sources )
        {
            appliedTransactions[i] = Pair.of( ds.getName(), ds.getLastCommittedTxId() );
            try
            {
                ds.rotateLogicalLog();
            }
            catch ( IOException e )
            {
                // TODO: what about error message?
                return new FailedResponse<Void>();
            }
        }

        context = new SlaveContext( context.machineId(), context.getEventIdentifier(), appliedTransactions );

        File baseDir = getBaseDir();

        for ( XaDataSource ds : sources )
        {
            for ( File storefile : ds.listStoreFiles() )
            {
                try
                {
                    FileInputStream stream = new FileInputStream( storefile );
                    try
                    {
                        writer.write( relativePath( baseDir, storefile ), stream.getChannel() );
                    }
                    finally
                    {
                        stream.close();
                    }
                }
                catch ( IOException e )
                {
                    // TODO: what about error message?
                    return new FailedResponse<Void>();
                }
            }
            writer.done();
        }
        return packResponse( context, null, ALL );
    }

    private File getBaseDir()
    {
        XaDataSourceManager mgr = graphDbConfig.getTxModule().getXaDataSourceManager();
        NeoStoreXaDataSource nioneodb = (NeoStoreXaDataSource) mgr.getXaDataSource( "nioneodb" );
        try
        {
            return new File( nioneodb.getStoreDir() ).getCanonicalFile().getAbsoluteFile();
        }
        catch ( IOException e )
        {
            return new File( nioneodb.getStoreDir() ).getAbsoluteFile();
        }
    }

    private String relativePath( File baseDir, File storeFile ) throws FileNotFoundException
    {
        String prefix = baseDir.getAbsolutePath();
        String path = storeFile.getAbsolutePath();
        if ( !path.startsWith( prefix ) ) throw new FileNotFoundException();
        path = path.substring( prefix.length() );
        if ( path.startsWith( "/" ) ) return path.substring( 1 );
        return path;
    }

    private static interface LockGrabber
    {
        void grab( LockManager lockManager, LockReleaser lockReleaser, Object entity );
    }

    private static LockGrabber READ_LOCK_GRABBER = new LockGrabber()
    {
        public void grab( LockManager lockManager, LockReleaser lockReleaser, Object entity )
        {
            lockManager.getReadLock( entity );
            lockReleaser.addLockToTransaction( entity, LockType.READ );
        }
    };

    private static LockGrabber WRITE_LOCK_GRABBER = new LockGrabber()
    {
        public void grab( LockManager lockManager, LockReleaser lockReleaser, Object entity )
        {
            lockManager.getWriteLock( entity );
            lockReleaser.addLockToTransaction( entity, LockType.WRITE );
        }
    };

    // =====================================================================
    // Just some methods which aren't really used when running a HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

    public Map<Integer, Collection<SlaveContext>> getOngoingTransactions()
    {
        Map<Integer, Collection<SlaveContext>> result = new HashMap<Integer, Collection<SlaveContext>>();
        for ( SlaveContext context : transactions.keySet() )
        {
            Collection<SlaveContext> txs = result.get( context.machineId() );
            if ( txs == null )
            {
                txs = new ArrayList<SlaveContext>();
                result.put( context.machineId(), txs );
            }
            txs.add( context );
        }
        return result;
    }
}
