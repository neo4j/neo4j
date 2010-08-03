package slavetest;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.ha.FailedResponse;
import org.neo4j.kernel.impl.ha.IdAllocation;
import org.neo4j.kernel.impl.ha.LockResult;
import org.neo4j.kernel.impl.ha.LockStatus;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.Response;
import org.neo4j.kernel.impl.ha.SlaveContext;
import org.neo4j.kernel.impl.ha.TransactionStream;
import org.neo4j.kernel.impl.ha.TransactionStreams;
import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class FakeMaster implements Master
{
    private static final Predicate<Long> ALL = new Predicate<Long>()
    {
        public boolean accept( Long item )
        {
            return true;
        }
    };
    
    private final GraphDatabaseService graphDb;
    private final Map<TxIdElement, Transaction> transactions =
            new HashMap<TxIdElement, Transaction>();
    private final TransactionManager txManager;
    
    FakeMaster( String path )
    {
        graphDb = new EmbeddedGraphDatabase( path );
        txManager = getConfig().getTxModule().getTxManager();
        
    }
    
    public Response<LockResult> acquireReadLock( SlaveContext context, int eventIdentifier,
            Node... nodes )
    {
        TxIdElement tx = new TxIdElement( context.slaveId(), eventIdentifier );
        Transaction otherTx = suspendOtherAndResumeThis( tx );
        try
        {
            LockManager lockManager = getConfig().getLockManager();
            LockReleaser lockReleaser = getConfig().getLockReleaser();
            for ( Node node : nodes )
            {
                lockManager.getReadLock( node );
                lockReleaser.addLockToTransaction( node, LockType.READ );
            }
            return new Response<LockResult>( new LockResult( LockStatus.OK_LOCKED ),
                    new TransactionStreams() );
        }
        catch ( DeadlockDetectedException e )
        {
            return new Response<LockResult>( new LockResult( e.getMessage() ),
                    new TransactionStreams() );
        }
        catch ( IllegalResourceException e )
        {
            return new Response<LockResult>( new LockResult( LockStatus.NOT_LOCKED ),
                    new TransactionStreams() );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }
    
    private Transaction getOrBeginTx( TxIdElement txId )
    {
        try
        {
            Transaction tx = transactions.get( txId );
            if ( tx == null )
            {
                txManager.begin();
                tx = txManager.getTransaction();
                transactions.put( txId, tx );
            }
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

    Transaction suspendOtherAndResumeThis( TxIdElement txId )
    {
        try
        {
            Transaction otherTx = txManager.getTransaction();
            Transaction transaction = getOrBeginTx( txId );
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
//                    beginTransaction();
                    throw new RuntimeException( "Shouldn't happen, right?" );
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
    
    void suspendThisAndResumeOther( Transaction otherTx )
    {
        try
        {
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

    void rollbackThisAndResumeOther( Transaction otherTx )
    {
        try
        {
            txManager.rollback();
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
    
    public Response<LockResult> acquireWriteLock( SlaveContext context, int eventIdentifier,
            Node... nodes )
    {
        TxIdElement tx = new TxIdElement( context.slaveId(), eventIdentifier );
        Transaction otherTx = suspendOtherAndResumeThis( tx );
        try
        {
            LockManager lockManager = getConfig().getLockManager();
            LockReleaser lockReleaser = getConfig().getLockReleaser();
            for ( Node node : nodes )
            {
                lockManager.getWriteLock( node );
                lockReleaser.addLockToTransaction( node, LockType.WRITE );
            }
            return new Response<LockResult>( new LockResult( LockStatus.OK_LOCKED ),
                    new TransactionStreams() );
        }
        catch ( DeadlockDetectedException e )
        {
            return new Response<LockResult>( new LockResult( e.getMessage() ),
                    new TransactionStreams() );
        }
        catch ( IllegalResourceException e )
        {
            return new Response<LockResult>( new LockResult( LockStatus.NOT_LOCKED ),
                    new TransactionStreams() );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }

    public Response<LockResult> acquireReadLock( SlaveContext context, int eventIdentifier,
            Relationship... relationships )
    {
        TxIdElement tx = new TxIdElement( context.slaveId(), eventIdentifier );
        Transaction otherTx = suspendOtherAndResumeThis( tx );
        try
        {
            LockManager lockManager = getConfig().getLockManager();
            LockReleaser lockReleaser = getConfig().getLockReleaser();
            for ( Relationship relationship : relationships )
            {
                lockManager.getReadLock( relationship );
                lockReleaser.addLockToTransaction( relationship, LockType.READ );
            }
            return new Response<LockResult>( new LockResult( LockStatus.OK_LOCKED ),
                    new TransactionStreams() );
        }
        catch ( DeadlockDetectedException e )
        {
            return new Response<LockResult>( new LockResult( e.getMessage() ),
                    new TransactionStreams() );
        }
        catch ( IllegalResourceException e )
        {
            return new Response<LockResult>( new LockResult( LockStatus.NOT_LOCKED ),
                    new TransactionStreams() );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }

    public Response<LockResult> acquireWriteLock( SlaveContext context, int eventIdentifier,
            Relationship... relationships )
    {
        TxIdElement tx = new TxIdElement( context.slaveId(), eventIdentifier );
        Transaction otherTx = suspendOtherAndResumeThis( tx );
        try
        {
            LockManager lockManager = getConfig().getLockManager();
            LockReleaser lockReleaser = getConfig().getLockReleaser();
            for ( Relationship relationship : relationships )
            {
                lockManager.getWriteLock( relationship );
                lockReleaser.addLockToTransaction( relationship, LockType.WRITE );
            }
            return new Response<LockResult>( new LockResult( LockStatus.OK_LOCKED ),
                    new TransactionStreams() );
        }
        catch ( DeadlockDetectedException e )
        {
            return new Response<LockResult>( new LockResult( e.getMessage() ),
                    new TransactionStreams() );
        }
        catch ( IllegalResourceException e )
        {
            return new Response<LockResult>( new LockResult( LockStatus.NOT_LOCKED ),
                    new TransactionStreams() );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }
    
    private Config getConfig()
    {
        return ((EmbeddedGraphDatabase) graphDb).getConfig();
    }
    
    public Response<IdAllocation> allocateIds( SlaveContext context, IdType idType )
    {
        IdGeneratorFactory factory = getConfig().getIdGeneratorFactory();
        IdGenerator generator = factory.get( idType );
        int size = 10;
        long[] ids = new long[size];
        for ( int i = 0; i < size; i++ )
        {
            ids[i] = generator.nextId();
        }
        return new Response<IdAllocation>( new IdAllocation( ids, generator.getHighId(),
                generator.getDefragCount() ), new TransactionStreams() );
    }

    public Response<Long> commitSingleResourceTransaction( SlaveContext context,
            int eventIdentifier, String resource, TransactionStream transactionStream )
    {
        TxIdElement tx = new TxIdElement( context.slaveId(), eventIdentifier );
        Transaction otherTx = suspendOtherAndResumeThis( tx );
        try
        {
            XaDataSource dataSource = getConfig().getTxModule().getXaDataSourceManager()
                    .getXaDataSource( resource );
            // Always exactly one transaction (ReadableByteChannel)
            final long txId = dataSource.applyPreparedTransaction(
                    transactionStream.getChannels().iterator().next() );
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
            return new FailedResponse<Long>();
        }
        finally
        {
            // Since the master-transaction carries no actual state, just locks
            // we would like to release the locks... and it's best done by just
            // rolling back the tx
            rollbackThisAndResumeOther( otherTx );
        }
    }

    public Response<Integer> createRelationshipType( SlaveContext context, String name )
    {
        // Does this type exist locally?
        Integer id = getConfig().getRelationshipTypeHolder().getIdFor( name );
        if ( id != null )
        {
            // OK, return
            return new Response<Integer>( id, new TransactionStreams() );
        }
        
        // No? Create it then
        Config config = getConfig();
        id = config.getRelationshipTypeCreator().getOrCreate( txManager,
                config.getIdGeneratorModule().getIdGenerator(),
                config.getPersistenceModule().getPersistenceManager(), name );
        return packResponse( context, id, ALL );
    }

    private <T> Response<T> packResponse( SlaveContext context, T response,
            Predicate<Long> filter )
    {
        try
        {
            TransactionStreams streams = new TransactionStreams();
            for ( Map.Entry<String, Long> slaveEntry :
                    context.lastAppliedTransactions().entrySet() )
            {
                String resourceName = slaveEntry.getKey();
                XaDataSource dataSource = getConfig().getTxModule()
                        .getXaDataSourceManager().getXaDataSource( resourceName );
                long masterLastTx = dataSource.getLastCommittedTxId();
                long slaveLastTx = slaveEntry.getValue();
                Collection<ReadableByteChannel> channels = new ArrayList<ReadableByteChannel>();
                for ( long txId = slaveLastTx+1; txId <= masterLastTx; txId++ )
                {
                    channels.add( dataSource.getCommittedTransaction( txId ) );
                }
                streams.add( slaveEntry.getKey(), new TransactionStream( channels ) );
            }
            return new Response<T>( response, new TransactionStreams() );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            return new FailedResponse<T>();
        }
    }

    public Response<Void> pullUpdates( SlaveContext context )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Response<Void> rollbackTransaction( SlaveContext context, int eventIdentifier )
    {
        TxIdElement txId = new TxIdElement( context.slaveId(), eventIdentifier );
        Transaction otherTx = suspendOtherAndResumeThis( txId );
        try
        {
            Transaction tx = transactions.get( txId );
            if ( tx == null )
            {
                throw new RuntimeException( "Shouldn't happen" );
            }
            txManager.rollback();
            return new Response<Void>( null, new TransactionStreams() );
        }
        catch ( IllegalStateException e )
        {
            throw new RuntimeException( e );
        }
        catch ( SecurityException e )
        {
            throw new RuntimeException( e );
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }
    
    private static final class TxIdElement
    {
        private final int slaveId;
        private final int eventIdentifier;
        private final int hashCode;
        
        TxIdElement( int slaveId, int eventIdentifier )
        {
            this.slaveId = slaveId;
            this.eventIdentifier = eventIdentifier;
            this.hashCode = calculateHashCode();
        }

        private int calculateHashCode()
        {
            return (slaveId << 20) | eventIdentifier;
        }
        
        @Override
        public int hashCode()
        {
            return hashCode;
        }
        
        @Override
        public boolean equals( Object obj )
        {
            TxIdElement other = (TxIdElement) obj;
            return other.slaveId == slaveId && other.eventIdentifier == eventIdentifier;
        }
    }
}
