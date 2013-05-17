/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.ShutdownXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleStatus;

/**
 * All datasources that have been defined in the XA data source configuration
 * file or manually added will be created and registered here. A mapping between
 * "name", "data source" and "branch id" is kept by this manager.
 * <p/>
 * Use the {@link #getXaDataSource} to obtain the instance of a datasource that
 * has been defined in the XA data source configuration.
 *
 * @see XaDataSource
 */
public class XaDataSourceManager
        implements Lifecycle
{
    // key = data source name, value = data source
    private final Map<String, XaDataSource> dataSources =
            new HashMap<String, XaDataSource>();
    // key = branchId, value = data source
    private final Map<String, XaDataSource> branchIdMapping =
            new HashMap<String, XaDataSource>();
    // key = data source name, value = branchId
    private final Map<String, byte[]> sourceIdMapping =
            new HashMap<String, byte[]>();
    private Iterable<DataSourceRegistrationListener> dsRegistrationListeners = Listeners.newListeners();
    private LifeSupport life = new LifeSupport();

    private final StringLogger msgLog;
    private boolean isShutdown = false;

    public XaDataSourceManager( StringLogger msgLog )
    {
        this.msgLog = msgLog;
    }

    public void addDataSourceRegistrationListener( DataSourceRegistrationListener listener )
    {
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            try
            {
                for ( XaDataSource ds : dataSources.values() )
                {
                    listener.registeredDataSource( ds );
                }
            }
            catch ( Throwable t )
            {
                msgLog.logMessage( "Failed when notifying registering listener", t );
            }
        }
        dsRegistrationListeners = Listeners.addListener( listener, dsRegistrationListeners );
    }

    public void removeDataSourceRegistrationListener( DataSourceRegistrationListener
                                                              dataSourceRegistrationListener )
    {
        dsRegistrationListeners = Listeners.removeListener( dataSourceRegistrationListener, dsRegistrationListeners );
    }

    @Override
    public void init()
            throws Throwable
    {
        if (dsRegistrationListeners == null)
        {
            dsRegistrationListeners = Listeners.newListeners();
        }
    }

    @Override
    public void start()
            throws Throwable
    {
        life = new LifeSupport();
        for ( XaDataSource ds : dataSources.values() )
        {
            life.add( ds );
        }
        life.start();
        for ( DataSourceRegistrationListener listener : dsRegistrationListeners )
        {
            try
            {
                for ( XaDataSource ds : dataSources.values() )
                {
                    listener.registeredDataSource( ds );
                }
            }
            catch ( Throwable t )
            {
                msgLog.logMessage( "Failed when notifying registering listener", t );
            }
        }
    }

    @Override
    public void stop()
            throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown()
            throws Throwable
    {
        dsRegistrationListeners = null;
        life.shutdown();
        dataSources.clear();
        branchIdMapping.clear();
        sourceIdMapping.clear();
        isShutdown = true;
    }

    /**
     * Returns the {@link org.neo4j.kernel.impl.transaction.xaframework.XaDataSource}
     * registered as <CODE>name</CODE>. If no data source is registered with
     * that name <CODE>null</CODE> is returned.
     *
     * @param name the name of the data source
     */
    public XaDataSource getXaDataSource( String name )
    {
        if ( isShutdown )
        {
            return new ShutdownXaDataSource();
        }

        return dataSources.get( name );
    }

    /**
     * Used to access the Neo DataSource. This should be replaced with
     * DataSource registration listeners instead, since this DataSource is not
     * always guaranteed to return anything (in HA case).
     */
    @Deprecated
    public NeoStoreXaDataSource getNeoStoreDataSource()
    {
        return (NeoStoreXaDataSource) getXaDataSource( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
    }

    /**
     * Public for testing purpose. Do not use.
     */
    public synchronized void registerDataSource( final XaDataSource dataSource )
    {
        dataSources.put( dataSource.getName(), dataSource );
        branchIdMapping.put( UTF8.decode( dataSource.getBranchId() ), dataSource );
        sourceIdMapping.put( dataSource.getName(), dataSource.getBranchId() );
        life.add( dataSource );
        if ( life.getStatus().equals( LifecycleStatus.STARTED ) )
        {
            Listeners.notifyListeners( dsRegistrationListeners, new Listeners.Notification<DataSourceRegistrationListener>()
            {
                @Override
                public void notify( DataSourceRegistrationListener listener )
                {
                    listener.registeredDataSource( dataSource );
                }
            } );
        }
    }

    /**
     * Public for testing purpose. Do not use.
     */
    public synchronized void unregisterDataSource( String name )
    {
        final XaDataSource dataSource = dataSources.get( name );
        if ( dataSource == null )
        {
            return;
        }

        byte branchId[] = getBranchId(
                dataSource.getXaConnection().getXaResource() );
        dataSources.remove( name );
        branchIdMapping.remove( UTF8.decode( branchId ) );
        sourceIdMapping.remove( name );
        Listeners.notifyListeners( dsRegistrationListeners, new Listeners.Notification<DataSourceRegistrationListener>()
        {
            @Override
            public void notify( DataSourceRegistrationListener listener )
            {
                listener.unregisteredDataSource( dataSource );
            }
        } );
        life.remove( dataSource );
        // No need for shutdown, removing does that
    }

    synchronized byte[] getBranchId( XAResource xaResource )
    {
        if ( xaResource instanceof XaResource )
        {
            byte branchId[] = ((XaResource) xaResource).getBranchId();
            if ( branchId != null )
            {
                return branchId;
            }
        }
        for ( Map.Entry<String, XaDataSource> entry : dataSources.entrySet() )
        {
            XaDataSource dataSource = entry.getValue();
            XAResource resource = dataSource.getXaConnection().getXaResource();
            try
            {
                if ( resource.isSameRM( xaResource ) )
                {
                    String name = entry.getKey();
                    return sourceIdMapping.get( name );
                }
            }
            catch ( XAException e )
            {
                throw new TransactionFailureException(
                        "Unable to check is same resource", e );
            }
        }
        throw new TransactionFailureException(
                "Unable to find mapping for XAResource[" + xaResource + "]" );
    }

    private XaDataSource getDataSource( byte branchId[] )
    {
        XaDataSource dataSource = branchIdMapping.get( UTF8.decode( branchId ) );
        if ( dataSource == null )
        {
            throw new TransactionFailureException(
                    "No mapping found for branchId[0x" +
                            UTF8.decode( branchId ) + "]" );
        }
        return dataSource;
    }

    // not thread safe
    public Collection<XaDataSource> getAllRegisteredDataSources()
    {
        return dataSources.values();
    }

    /**
     * Recover all datasources
     */
    public void recover( Iterator<List<TxLog.Record>> knownDanglingRecordList )
    {
        // contains NonCompletedTransaction that needs to be committed
        List<NonCompletedTransaction> commitList =
                new ArrayList<NonCompletedTransaction>();

        // contains Xids that should be rolledback
        final List<Xid> rollbackList = new LinkedList<Xid>();

        // key = Resource(branchId) value = XAResource
        final Map<Resource, XaDataSource> resourceMap =
                new HashMap<Resource, XaDataSource>();
        buildRecoveryInfo( commitList, rollbackList, resourceMap,
                knownDanglingRecordList );
        // invoke recover on all xa resources found
        final List<Xid> recoveredXidsList = new LinkedList<Xid>();

        try
        {

            for ( XaDataSource xaDataSource : dataSources.values() )
            {
                XAResource xaRes = xaDataSource.getXaConnection().getXaResource();
                Xid xids[] = xaRes.recover( XAResource.TMNOFLAGS );

                for ( Xid xid : xids )
                {
                    if ( XidImpl.isThisTm( xid.getGlobalTransactionId() ) )
                    {
                        // linear search
                        if ( rollbackList.contains( xid ) )
                        {
                            msgLog.logMessage( "TM: Found pre commit " + xid + " rolling back ... ", true );
                            rollbackList.remove( xid );
                            xaRes.rollback( xid );
                        }
                        else
                        {
                            Resource resource = new Resource( xid.getBranchQualifier() );
                            if ( !resourceMap.containsKey( resource ) )
                            {
                                resourceMap.put( resource, xaDataSource );
                            }
                            recoveredXidsList.add( xid );
                        }
                    }
                    else
                    {
                        msgLog.warn( "Unknown xid: " + xid );
                    }
                }
            }

            // sort the commit list after sequence number
            Collections.sort( commitList );

            // go through and commit
            for ( NonCompletedTransaction nct : commitList )
            {
                int seq = nct.getSequenceNumber();
                Xid xids[] = nct.getXids();
                msgLog.debug( "Marked as commit tx-seq[" + seq +
                              "] branch length: " + xids.length );
                for ( Xid xid : xids )
                {
                    if ( !recoveredXidsList.contains( xid ) )
                    {
                        msgLog.debug( "Tx-seq[" + seq + "][" + xid +
                                      "] not found in recovered xid list, "
                                      + "assuming already committed" );
                        continue;
                    }
                    recoveredXidsList.remove( xid );
                    Resource resource = new Resource( xid.getBranchQualifier() );
                    if ( !resourceMap.containsKey( resource ) )
                    {
                        final TransactionFailureException ex = new TransactionFailureException(
                                "Couldn't find XAResource for " + xid );
                        throw logAndReturn( "TM: recovery error", ex );
                    }
                    msgLog.debug( "TM: Committing tx " + xid );
                    resourceMap.get( resource ).getXaConnection().getXaResource().commit( xid, false );
                }
            }

            // rollback the rest
            for ( Xid xid : recoveredXidsList )
            {
                Resource resource = new Resource( xid.getBranchQualifier() );
                if ( !resourceMap.containsKey( resource ) )
                {
                    final TransactionFailureException ex = new TransactionFailureException(
                            "Couldn't find XAResource for " + xid );
                    throw logAndReturn( "TM: recovery error", ex );
                }
                msgLog.debug( "TM: no match found for " + xid + " removing" );
                resourceMap.get( resource ).getXaConnection().getXaResource().rollback( xid );
            }
            if ( rollbackList.size() > 0 )
            {
                msgLog.debug( "TxLog contained unresolved "
                        + "xids that needed rollback. They couldn't be matched to "
                        + "any of the XAResources recover list. " + "Assuming "
                        + rollbackList.size()
                        + " transactions already rolled back." );
            }

            // Rotate the logs of the participated data sources, making sure that
            // done-records are written so that even if the tm log gets truncated,
            // which it will be after this recovery, that transaction information
            // doesn't get lost.
            for ( XaDataSource participant : MapUtil.reverse( resourceMap ).keySet() )
            {
                participant.rotateLogicalLog();
            }
        }
        catch ( IOException e )
        {
            throw logAndReturn( "TM: recovery failed", new TransactionFailureException( "Recovery failed.", e ) );
        }
        catch ( XAException e )
        {
            throw logAndReturn( "TM: recovery failed", new TransactionFailureException( "Recovery failed.", e ) );
        }
    }

    private void buildRecoveryInfo( List<NonCompletedTransaction> commitList,
                                    List<Xid> rollbackList, Map<Resource, XaDataSource> resourceMap,
                                    Iterator<List<TxLog.Record>> danglingRecordList
    )
    {
        while ( danglingRecordList.hasNext() )
        {
            Iterator<TxLog.Record> dListItr =
                    danglingRecordList.next().iterator();
            TxLog.Record startRecord = dListItr.next();
            if ( startRecord.getType() != TxLog.TX_START )
            {
                throw logAndReturn( "TM error building recovery info",
                        new TransactionFailureException(
                                "First record not a start record, type="
                                        + startRecord.getType() ) );
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

                        throw logAndReturn( "TM error building recovery info", new TransactionFailureException(
                                "Already marked commit " + startRecord ) );
                    }
                    branchSet.add( new Resource( record.getBranchId() ) );
                }
                else if ( record.getType() == TxLog.MARK_COMMIT )
                {
                    if ( markedCommit != -1 )
                    {
                        throw logAndReturn( "TM error building recovery info", new TransactionFailureException(
                                "Already marked commit " + startRecord ) );
                    }
                    markedCommit = record.getSequenceNumber();
                }
                else
                {
                    throw logAndReturn( "TM error building recovery info", new TransactionFailureException(
                            "Illegal record type[" + record.getType() + "]" ) );
                }
            }
            Iterator<Resource> resourceItr = branchSet.iterator();
            List<Xid> xids = new LinkedList<Xid>();
            while ( resourceItr.hasNext() )
            {
                Resource resource = resourceItr.next();
                if ( !resourceMap.containsKey( resource ) )
                {
                    resourceMap.put( resource, getDataSource( resource.getResourceId() ) );
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

    private <E extends Exception> E logAndReturn( String msg, E exception )
    {
        try
        {
            msgLog.logMessage( msg, exception, true );
            return exception;
        }
        catch ( Throwable t )
        {
            return exception;
        }
    }

    public void rotateLogicalLogs()
    {
        for ( XaDataSource dataSource : dataSources.values() )
        {
            try
            {
                dataSource.rotateLogicalLog();
            }
            catch ( IOException e )
            {
                msgLog.logMessage( "Couldn't rotate logical log for " + dataSource.getName(), e );
            }
        }
    }

    private static class NonCompletedTransaction
            implements Comparable<NonCompletedTransaction>
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
            return xidList.toArray( new Xid[xidList.size()] );
        }

        @Override
        public String toString()
        {
            return "NonCompletedTx[" + seqNr + "," + xidList + "]";
        }

        @Override
        public int compareTo( NonCompletedTransaction nct )
        {
            return getSequenceNumber() - nct.getSequenceNumber();
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

        @Override
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

        @Override
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
}