/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.neo4j.impl.nioneo.store.StoreFailureException;
import org.neo4j.impl.transaction.xaframework.XaResource;
import org.neo4j.impl.util.ArrayMap;

/**
 * Public for testing purpose only. Use {@link TransactionFactory} to get a
 * <CODE>UserTransaction</CODE>, don't use this class.
 * <p>
 */
public class TxManager implements TransactionManager
{
    private static Logger log = Logger.getLogger( TxManager.class.getName() );

    private ArrayMap<Thread,TransactionImpl> txThreadMap;

    private final String txLogDir;
    private String separator = "/";
    private String logSwitcherFileName = "active_tx_log";
    private String txLog1FileName = "tm_tx_log.1";
    private String txLog2FileName = "tm_tx_log.2";
    private int maxTxLogRecordCount = 1000;
    private int eventIdentifierCounter = 0;

    private TxLog txLog = null;
    private XaDataSourceManager xaDsManager = null;
    private boolean tmOk = false;

    TxManager( String txLogDir )
    {
        this.txLogDir = txLogDir;
    }

    synchronized int getNextEventIdentifier()
    {
        return eventIdentifierCounter++;
    }

    void stop()
    {
        if ( txLog != null )
        {
            try
            {
                txLog.close();
            }
            catch ( IOException e )
            {
                log.warning( "Unable to close tx log[" + txLog.getName() + "]"
                    + ", " + e );
            }
        }
    }

    void init( XaDataSourceManager xaDsManagerToUse )
    {
        this.xaDsManager = xaDsManagerToUse;
        txThreadMap = new ArrayMap<Thread,TransactionImpl>( 5, true, true );
        separator = System.getProperty( "file.separator" );
        logSwitcherFileName = txLogDir + separator + "active_tx_log";
        txLog1FileName = "tm_tx_log.1";
        txLog2FileName = "tm_tx_log.2";
        try
        {
            if ( new File( logSwitcherFileName ).exists() )
            {
                FileChannel fc = new RandomAccessFile( logSwitcherFileName,
                    "rw" ).getChannel();
                byte fileName[] = new byte[256];
                ByteBuffer buf = ByteBuffer.wrap( fileName );
                fc.read( buf );
                fc.close();
                String currentTxLog = txLogDir + separator
                    + new String( fileName ).trim();
                if ( !new File( currentTxLog ).exists() )
                {
                    throw new RuntimeException( "Unable to start TM, "
                        + "active tx log file[" + currentTxLog + 
                        "] not found." );
                }
                txLog = new TxLog( currentTxLog );
            }
            else
            {
                if ( new File( txLogDir + separator + txLog1FileName ).exists()
                    || new File( txLogDir + separator + txLog2FileName )
                        .exists() )
                {
                    throw new RuntimeException( "Unable to start TM, "
                        + "no active tx log file found but found either "
                        + txLog1FileName + " or " + txLog2FileName
                        + " file, please set one of them as active or "
                        + "remove them." );
                }
                ByteBuffer buf = ByteBuffer.wrap( txLog1FileName
                    .getBytes( "UTF-8" ) );
                FileChannel fc = new RandomAccessFile( logSwitcherFileName,
                    "rw" ).getChannel();
                fc.write( buf );
                txLog = new TxLog( txLogDir + separator + txLog1FileName );
                fc.force( true );
                fc.close();
            }
            Iterator<List<TxLog.Record>> danglingRecordList = 
                txLog.getDanglingRecords();
            if ( danglingRecordList.hasNext() )
            {
                log.info( "Unresolved transactions found, " + 
                    "recovery started ..." );
                recover( danglingRecordList );
                log.info( "Recovery completed, all transactions have been " + 
                    "resolved to a consistent state." );
            }
            getTxLog().truncate();
            tmOk = true;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            log.severe( "Unable to start TM" );
            throw new RuntimeException( e );
        }
    }

    synchronized TxLog getTxLog() throws IOException
    {
        if ( txLog.getRecordCount() > maxTxLogRecordCount )
        {
            if ( txLog.getName().endsWith( txLog1FileName ) )
            {
                txLog.switchToLogFile( txLogDir + separator + txLog2FileName );
                changeActiveLog( txLog2FileName );
            }
            else if ( txLog.getName().endsWith( txLog2FileName ) )
            {
                txLog.switchToLogFile( txLogDir + separator + txLog1FileName );
                changeActiveLog( txLog1FileName );
            }
            else
            {
                tmOk = false;
                log.severe( "Unknown active tx log file[" + txLog.getName()
                    + "], unable to switch." );
                throw new IOException( "Unknown txLogFile[" + txLog.getName()
                    + "] not equals to either [" + txLog1FileName + "] or ["
                    + txLog2FileName + "]" );
            }
        }
        return txLog;
    }

    private void changeActiveLog( String newFileName ) throws IOException
    {
        // change active log
        FileChannel fc = new RandomAccessFile( logSwitcherFileName, "rw" )
            .getChannel();
        ByteBuffer buf = ByteBuffer.wrap( newFileName.getBytes() );
        fc.truncate( 0 );
        fc.write( buf );
        fc.force( true );
        fc.close();
    }

    void setTmNotOk()
    {
        tmOk = false;
    }

    private void recover( Iterator<List<TxLog.Record>> danglingRecordList )
    {
        try
        {
            // contains NonCompletedTransaction that needs to be committed
            List<NonCompletedTransaction> commitList = 
                new ArrayList<NonCompletedTransaction>();

            // contains Xids that should be rolledback
            List<Xid> rollbackList = new LinkedList<Xid>();

            // key = Resource(branchId) value = XAResource
            Map<Resource,XAResource> resourceMap = 
                new HashMap<Resource,XAResource>();
            buildRecoveryInfo( commitList, rollbackList, resourceMap,
                danglingRecordList );
            // invoke recover on all xa resources found
            Iterator<Resource> resourceItr = resourceMap.keySet().iterator();
            List<Xid> recoveredXidsList = new LinkedList<Xid>();
            while ( resourceItr.hasNext() )
            {
                XAResource xaRes = resourceMap.get( resourceItr.next() );
                Xid xids[] = xaRes.recover( XAResource.TMNOFLAGS );
                for ( int i = 0; i < xids.length; i++ )
                {
                    if ( XidImpl.isThisTm( xids[i].getGlobalTransactionId() ) )
                    {
                        // linear search
                        if ( rollbackList.contains( xids[i] ) )
                        {
                            log.fine( "Found pre commit " + xids[i]
                                + " rolling back ... " );
                            rollbackList.remove( xids[i] );
                            xaRes.rollback( xids[i] );
                        }
                        else
                        {
                            recoveredXidsList.add( xids[i] );
                        }
                    }
                    else
                    {
                        log.warning( "Unknown xid: " + xids[i] );
                    }
                }
            }
            // sort the commit list after sequence number
            Collections.sort( commitList,
                new Comparator<NonCompletedTransaction>()
                {
                    public int compare( NonCompletedTransaction r1,
                        NonCompletedTransaction r2 )
                    {
                        return r1.getSequenceNumber() - r2.getSequenceNumber();
                    }
                } );
            // go through and commit
            Iterator<NonCompletedTransaction> commitItr = commitList.iterator();
            while ( commitItr.hasNext() )
            {
                NonCompletedTransaction nct = commitItr.next();
                int seq = nct.getSequenceNumber();
                Xid xids[] = nct.getXids();
                log.fine( "Marked as commit tx-seq[" + seq + 
                    "] branch length: " + xids.length );
                for ( int i = 0; i < xids.length; i++ )
                {
                    if ( !recoveredXidsList.contains( xids[i] ) )
                    {
                        log.fine( "Tx-seq[" + seq + "][" + xids[i] + 
                            "] not found in recovered xid list, "
                            + "assuming already committed" );
                        continue;
                    }
                    recoveredXidsList.remove( xids[i] );
                    Resource resource = new Resource( xids[i]
                        .getBranchQualifier() );
                    if ( !resourceMap.containsKey( resource ) )
                    {
                        throw new RuntimeException(
                            "Couldn't find XAResource for " + xids[i] );
                    }
                    log.fine( "Commiting tx seq[" + seq + "][" + 
                        xids[i] + "] ... " );
                    resourceMap.get( resource ).commit( xids[i], false );
                }
            }
            // rollback the rest
            Iterator<Xid> rollbackItr = recoveredXidsList.iterator();
            while ( rollbackItr.hasNext() )
            {
                Xid xid = rollbackItr.next();
                Resource resource = new Resource( xid.getBranchQualifier() );
                if ( !resourceMap.containsKey( resource ) )
                {
                    throw new RuntimeException( "Couldn't find XAResource for "
                        + xid );
                }
                log.fine( "Rollback " + xid + " ... " );
                resourceMap.get( resource ).rollback( xid );
            }
            if ( rollbackList.size() > 0 )
            {
                log.fine( "TxLog contained unresolved "
                    + "xids that needed rollback. They couldn't be matched to "
                    + "any of the XAResources recover list. " + "Assuming "
                    + rollbackList.size()
                    + " transactions already rolled back." );
            }
        }
        catch ( XAException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void buildRecoveryInfo( List<NonCompletedTransaction> commitList,
        List<Xid> rollbackList, Map<Resource,XAResource> resourceMap,
        Iterator<List<TxLog.Record>> danglingRecordList )
    {
        while ( danglingRecordList.hasNext() )
        {
            Iterator<TxLog.Record> dListItr = 
                danglingRecordList.next().iterator();
            TxLog.Record startRecord = dListItr.next();
            if ( startRecord.getType() != TxLog.TX_START )
            {
                throw new RuntimeException(
                    "First record not a start record, type="
                        + startRecord.getType() );
            }
            // get branches & commit status
            HashSet<Resource> branchSet = new HashSet<Resource>();
            int markedCommit = -1;
            while ( dListItr.hasNext() )
            {
                TxLog.Record record = dListItr.next();
                if ( record.getType() == TxLog.BRANCH_ADD )
                {
                    if ( markedCommit != -1 )
                    {
                        throw new RuntimeException( "Already marked commit "
                            + startRecord );
                    }
                    branchSet.add( new Resource( record.getBranchId() ) );
                }
                else if ( record.getType() == TxLog.MARK_COMMIT )
                {
                    if ( markedCommit != -1 )
                    {
                        throw new RuntimeException( "Already marked commit "
                            + startRecord );
                    }
                    markedCommit = record.getSequenceNumber();
                }
                else
                {
                    throw new RuntimeException( "Illegal record type["
                        + record.getType() + "]" );
                }
            }
            Iterator<Resource> resourceItr = branchSet.iterator();
            List<Xid> xids = new LinkedList<Xid>();
            while ( resourceItr.hasNext() )
            {
                Resource resource = resourceItr.next();
                if ( !resourceMap.containsKey( resource ) )
                {
                    resourceMap.put( resource, getXaResource( 
                        resource.getResourceId() ) );
                }
                xids.add( new XidImpl( startRecord.getGlobalId(), 
                    resource.getResourceId() ) );
            }
            if ( markedCommit != -1 ) // this xid needs to be committed
            {
                commitList.add( 
                    new NonCompletedTransaction( markedCommit, xids ) );
            }
            else
            {
                rollbackList.addAll( xids );
            }
        }

    }

    private static class NonCompletedTransaction
    {
        private int seqNr = -1;
        private List<Xid> xidList = null;

        NonCompletedTransaction( int seqNr, List<Xid> xidList )
        {
            this.seqNr = seqNr;
            this.xidList = xidList;
        }

        int getSequenceNumber()
        {
            return seqNr;
        }

        Xid[] getXids()
        {
            return xidList.toArray( new XidImpl[xidList.size()] );
        }
    }

    private static class Resource
    {
        private byte resourceId[] = null;

        Resource( byte resourceId[] )
        {
            if ( resourceId == null || resourceId.length == 0 )
            {
                throw new IllegalArgumentException( "Illegal resourceId" );
            }
            this.resourceId = resourceId;
        }

        byte[] getResourceId()
        {
            return resourceId;
        }

        public boolean equals( Object o )
        {
            if ( !(o instanceof Resource) )
            {
                return false;
            }
            byte otherResourceId[] = ((Resource) o).getResourceId();

            if ( resourceId.length != otherResourceId.length )
            {
                return false;
            }
            for ( int i = 0; i < resourceId.length; i++ )
            {
                if ( resourceId[i] != otherResourceId[i] )
                {
                    return false;
                }
            }
            return true;
        }

        private volatile int hashCode = 0;

        public int hashCode()
        {
            if ( hashCode == 0 )
            {
                int calcHash = 0;
                for ( int i = 0; i < resourceId.length; i++ )
                {
                    calcHash += resourceId[i] << i * 8;
                }
                hashCode = 3217 * calcHash;
            }
            return hashCode;
        }
    }

    public void begin() throws NotSupportedException, SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }

        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx != null )
        {
            throw new NotSupportedException(
                "Nested transactions not supported" );
        }
        tx = new TransactionImpl( this );
        txThreadMap.put( thread, tx );
        // start record written on resource enlistment
    }
    
    // called when a resource gets enlisted
    void writeStartRecord( byte globalId[] ) throws SystemException
    {
        try
        {
            getTxLog().txStart( globalId );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            log.severe( "Error writing transaction log" );
            tmOk = false;
            throw new SystemException( "TM encountered a problem, "
                + " error writing transaction log," + e );
        }
    }

    public void commit() throws RollbackException, HeuristicMixedException,
        HeuristicRollbackException, IllegalStateException, SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        if ( tx.getStatus() != Status.STATUS_ACTIVE
            && tx.getStatus() != Status.STATUS_MARKED_ROLLBACK )
        {
            throw new IllegalStateException( "Tx status is: "
                + getTxStatusAsString( tx.getStatus() ) );
        }
        tx.doBeforeCompletion();
        // delist resources?
        if ( tx.getStatus() == Status.STATUS_ACTIVE )
        {
            commit( thread, tx );
        }
        else if ( tx.getStatus() == Status.STATUS_MARKED_ROLLBACK )
        {
            rollbackCommit( thread, tx );
        }
        else
        {
            throw new IllegalStateException( "Tx status is: "
                + getTxStatusAsString( tx.getStatus() ) );
        }
    }
    
    private void commit( Thread thread, TransactionImpl tx )
        throws SystemException, HeuristicMixedException,
        HeuristicRollbackException
    {
        // mark as commit in log done TxImpl.doCommit()
        StoreFailureException sfe = null;
        int xaErrorCode = -1;
        int result = Status.STATUS_UNKNOWN;
        if ( tx.getResourceCount() == 0 )
        {
            tx.setStatus( Status.STATUS_COMMITTED );
            result = Status.STATUS_COMMITTED;
        }
        else
        {
            try
            {
                tx.doCommit();
                result = Status.STATUS_COMMITTED;
            }
            catch ( XAException e )
            {
                xaErrorCode = e.errorCode;
                e.printStackTrace();
                log.severe( "Commit failed, status="
                    + getTxStatusAsString( tx.getStatus() ) + ", errorCode="
                    + xaErrorCode );
                if ( tx.getStatus() == Status.STATUS_COMMITTED )
                {
                    // this should never be
                    tmOk = false;
                    throw new RuntimeException(
                        "commit threw exception but status is committed?", e );
                }
            }
            catch ( StoreFailureException e )
            {
                sfe = e;
                // ok we should have status not committed and
            }
        }
        if ( tx.getStatus() != Status.STATUS_COMMITTED )
        {
            try
            {
                tx.doRollback();
                result = Status.STATUS_ROLLEDBACK;
            }
            catch ( XAException e )
            {
                e.printStackTrace();
                log.severe( "Unable to rollback transaction. "
                    + "Some resources may be commited others not. "
                    + "Neo should be SHUTDOWN or FREEZED for "
                    + "resource maintance and transaction recovery ---->" );
                tmOk = false;
                throw new HeuristicMixedException(
                    "Unable to rollback ---> error code in commit: "
                        + xaErrorCode + " ---> error code for rollback: "
                        + e.errorCode );
            }
            tx.doAfterCompletion();
            txThreadMap.remove( thread );
            try
            {
                if ( tx.isGlobalStartRecordWritten() )
                {
                    getTxLog().txDone( tx.getGlobalId() );
                }
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                log.severe( "Error writing transaction log" );
                tmOk = false;
                throw new SystemException( "TM encountered a problem, "
                    + " error writing transaction log," + e );
            }
            tx.setStatus( Status.STATUS_NO_TRANSACTION );
            if ( sfe == null )
            {
                throw new HeuristicRollbackException(
                    "Failed to commit, transaction rolledback ---> "
                        + "error code was: " + xaErrorCode );
            }
            else
            {
                throw new HeuristicRollbackException(
                    "Failed to commit, transaction rolledback ---> " + sfe );
            }
        }
        tx.doAfterCompletion();
        txThreadMap.remove( thread );
        try
        {
            if ( tx.isGlobalStartRecordWritten() )
            {
                getTxLog().txDone( tx.getGlobalId() );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            log.severe( "Error writing transaction log" );
            tmOk = false;
            throw new SystemException( "TM encountered a problem, "
                + " error writing transaction log," + e );
        }
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
    }

    private void rollbackCommit( Thread thread, TransactionImpl tx )
        throws HeuristicMixedException, RollbackException, SystemException
    {
        try
        {
            tx.doRollback();
        }
        catch ( XAException e )
        {
            e.printStackTrace();
            log.severe( "Unable to rollback marked transaction. "
                + "Some resources may be commited others not. "
                + "Neo should be SHUTDOWN or FREEZED for "
                + "resource maintance and transaction recovery ---->" );
            tmOk = false;
            throw new HeuristicMixedException( "Unable to rollback "
                + " ---> error code for rollback: " + e.errorCode );
        }

        tx.doAfterCompletion();
        txThreadMap.remove( thread );
        try
        {
            if ( tx.isGlobalStartRecordWritten() )
            {
                getTxLog().txDone( tx.getGlobalId() );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            log.severe( "Error writing transaction log" );
            tmOk = false;
            throw new SystemException( "TM encountered a problem, "
                + " error writing transaction log," + e );
        }
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
        throw new RollbackException( 
            "Failed to commit, transaction rolledback" );
    }

    public void rollback() throws IllegalStateException, SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        if ( tx.getStatus() == Status.STATUS_ACTIVE ||
            tx.getStatus() == Status.STATUS_MARKED_ROLLBACK || 
            tx.getStatus() == Status.STATUS_PREPARING )
        {
            tx.doBeforeCompletion();
            // delist resources?
            try
            {
                tx.doRollback();
            }
            catch ( XAException e )
            {
                e.printStackTrace();
                log.severe( "Unable to rollback marked or active transaction. "
                    + "Some resources may be commited others not. "
                    + "Neo should be SHUTDOWN or FREEZED for "
                    + "resource maintance and transaction recovery ---->" );
                tmOk = false;
                throw new SystemException( "Unable to rollback "
                    + " ---> error code for rollback: " + e.errorCode );
            }
            tx.doAfterCompletion();
            txThreadMap.remove( thread );
            try
            {
                if ( tx.isGlobalStartRecordWritten() )
                {
                    getTxLog().txDone( tx.getGlobalId() );
                }
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                log.severe( "Error writing transaction log" );
                tmOk = false;
                throw new SystemException( "TM encountered a problem, "
                    + " error writing transaction log," + e );
            }
            tx.setStatus( Status.STATUS_NO_TRANSACTION );
        }
        else
        {
            throw new IllegalStateException( "Tx status is: "
                + getTxStatusAsString( tx.getStatus() ) );
        }
    }

    public int getStatus() // throws SystemException
    {
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx != null )
        {
            return tx.getStatus();
        }
        return Status.STATUS_NO_TRANSACTION;
    }

    public Transaction getTransaction()
    {
        return txThreadMap.get( Thread.currentThread() );
    }

    public void resume( Transaction tx ) throws IllegalStateException,
        SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }
        Thread thread = Thread.currentThread();
        if ( txThreadMap.get( thread ) != null )
        {
            throw new IllegalStateException( "Transaction already associated" );
        }
        if ( tx != null )
        {
            TransactionImpl txImpl = (TransactionImpl) tx;
            if ( txImpl.getStatus() != Status.STATUS_NO_TRANSACTION )
            {
                txImpl.markAsActive();
                txThreadMap.put( thread, txImpl );
            }
            // generate pro-active event resume
        }
    }

    public Transaction suspend() throws SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }
        // check for ACTIVE/MARKED_ROLLBACK?
        TransactionImpl tx = txThreadMap.remove( Thread.currentThread() );
        if ( tx != null )
        {
            // generate pro-active event suspend
            tx.markAsSuspended();
        }
        return tx;
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }
        Thread thread = Thread.currentThread();
        TransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        tx.setRollbackOnly();
    }

    public void setTransactionTimeout( int seconds ) throws SystemException
    {
        if ( !tmOk )
        {
            throw new SystemException( "TM has encountered some problem, "
                + "please perform neccesary action (tx recovery/restart)" );
        }
        // ...
    }

    byte[] getBranchId( XAResource xaRes )
    {
        if ( xaRes instanceof XaResource )
        {
            byte branchId[] = ((XaResource) xaRes).getBranchId();
            if ( branchId != null )
            {
                return branchId;
            }
        }
        return xaDsManager.getBranchId( xaRes );
    }

    XAResource getXaResource( byte branchId[] )
    {
        return xaDsManager.getXaResource( branchId );
    }

    String getTxStatusAsString( int status )
    {
        switch ( status )
        {
            case Status.STATUS_ACTIVE:
                return "STATUS_ACTIVE";
            case Status.STATUS_NO_TRANSACTION:
                return "STATUS_NO_TRANSACTION";
            case Status.STATUS_PREPARING:
                return "STATUS_PREPARING";
            case Status.STATUS_PREPARED:
                return "STATUS_PREPARED";
            case Status.STATUS_COMMITTING:
                return "STATUS_COMMITING";
            case Status.STATUS_COMMITTED:
                return "STATUS_COMMITED";
            case Status.STATUS_ROLLING_BACK:
                return "STATUS_ROLLING_BACK";
            case Status.STATUS_ROLLEDBACK:
                return "STATUS_ROLLEDBACK";
            case Status.STATUS_UNKNOWN:
                return "STATUS_UNKNOWN";
            case Status.STATUS_MARKED_ROLLBACK:
                return "STATUS_MARKED_ROLLBACK";
            default:
                return "STATUS_UNKNOWN(" + status + ")";
        }
    }

    public synchronized void dumpTransactions()
    {
        Iterator<TransactionImpl> itr = txThreadMap.values().iterator();
        if ( !itr.hasNext() )
        {
            System.out.println( "No uncompleted transactions" );
            return;
        }
        System.out.println( "Uncompleted transactions found: " );
        while ( itr.hasNext() )
        {
            System.out.println( itr.next() );
        }
    }

    public int getEventIdentifier()
    {
        TransactionImpl tx = (TransactionImpl) getTransaction();
        if ( tx != null )
        {
            return tx.getEventIdentifier();
        }
        return -1;
    }
}
