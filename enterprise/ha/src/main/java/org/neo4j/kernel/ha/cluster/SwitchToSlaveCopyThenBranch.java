/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.cluster;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.cluster.member.ClusterMemberAvailability;
import org.neo4j.com.storecopy.MoveAfterCopy;
import org.neo4j.com.storecopy.StoreCopyClient;
import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.BranchedDataPolicy;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.StoreUnableToParticipateInClusterException;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.slave.MasterClient;
import org.neo4j.kernel.ha.com.slave.MasterClientResolver;
import org.neo4j.kernel.ha.com.slave.SlaveServer;
import org.neo4j.kernel.ha.id.HaIdGeneratorFactory;
import org.neo4j.kernel.ha.store.ForeignStoreException;
import org.neo4j.kernel.ha.store.UnableToCopyStoreFromOldMasterException;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class SwitchToSlaveCopyThenBranch extends SwitchToSlave
{
    private final LogService logService;

    public SwitchToSlaveCopyThenBranch( File storeDir,
                                        LogService logService,
                                        FileSystemAbstraction fileSystemAbstraction,
                                        Config config,
                                        DependencyResolver resolver,
                                        HaIdGeneratorFactory idGeneratorFactory,
                                        DelegateInvocationHandler<Master> masterDelegateHandler,
                                        ClusterMemberAvailability clusterMemberAvailability,
                                        RequestContextFactory requestContextFactory,
                                        PullerFactory pullerFactory,
                                        Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                        MasterClientResolver masterClientResolver,
                                        SwitchToSlave.Monitor monitor,
                                        StoreCopyClientMonitor storeCopyMonitor,
                                        Supplier<NeoStoreDataSource> neoDataSourceSupplier,
                                        Supplier<TransactionIdStore> transactionIdStoreSupplier,
                                        Function<Slave, SlaveServer> slaveServerFactory,
                                        UpdatePuller updatePuller,
                                        PageCache pageCache,
                                        Monitors monitors,
                                        TransactionStats transactionCounters )
    {
        this( storeDir,
                logService,
                config,
                resolver,
                idGeneratorFactory,
                masterDelegateHandler,
                clusterMemberAvailability,
                requestContextFactory,
                pullerFactory,
                masterClientResolver,
                monitor,
                new StoreCopyClient( storeDir, config, kernelExtensions, logService.getUserLogProvider(),
                        fileSystemAbstraction, pageCache, storeCopyMonitor, false ),
                neoDataSourceSupplier,
                transactionIdStoreSupplier,
                slaveServerFactory,
                updatePuller,
                pageCache,
                monitors,
                transactionCounters
        );
    }

    SwitchToSlaveCopyThenBranch( File storeDir,
                                 LogService logService,
                                 Config config,
                                 DependencyResolver resolver,
                                 HaIdGeneratorFactory idGeneratorFactory,
                                 DelegateInvocationHandler<Master> masterDelegateHandler,
                                 ClusterMemberAvailability clusterMemberAvailability,
                                 RequestContextFactory requestContextFactory,
                                 PullerFactory pullerFactory,
                                 MasterClientResolver masterClientResolver,
                                 SwitchToSlave.Monitor monitor,
                                 StoreCopyClient storeCopyClient,
                                 Supplier<NeoStoreDataSource> neoDataSourceSupplier,
                                 Supplier<TransactionIdStore> transactionIdStoreSupplier,
                                 Function<Slave, SlaveServer> slaveServerFactory,
                                 UpdatePuller updatePuller,
                                 PageCache pageCache,
                                 Monitors monitors,
                                 TransactionStats transactionCounters )
    {
        super( idGeneratorFactory, resolver, monitors, requestContextFactory, masterDelegateHandler,
                clusterMemberAvailability, masterClientResolver, monitor, pullerFactory, updatePuller,
                slaveServerFactory, config, logService, pageCache, storeDir,
                transactionIdStoreSupplier,
                transactionCounters, neoDataSourceSupplier, storeCopyClient );
        this.logService = logService;
    }

    @Override
    void checkDataConsistency( MasterClient masterClient, TransactionIdStore txIdStore, StoreId storeId, URI
            masterUri, URI me, CancellationRequest cancellationRequest )
            throws Throwable
    {
        try
        {
            userLog.info( "Checking store consistency with master" );
            checkMyStoreIdAndMastersStoreId( storeId, masterUri, resolver );
            checkDataConsistencyWithMaster( masterUri, masterClient, storeId, txIdStore );
            userLog.info( "Store is consistent" );
        }
        catch ( StoreUnableToParticipateInClusterException upe )
        {
            userLog.info( "The store is inconsistent. Will treat it as branched and fetch a new one from the master" );
            msgLog.warn( "Current store is unable to participate in the cluster; fetching new store from master", upe );
            try
            {
                stopServicesAndHandleBranchedStore( config.get( HaSettings.branched_data_policy ), masterUri, me, cancellationRequest );
            }
            catch ( IOException e )
            {
                msgLog.warn( "Failed while trying to handle branched data", e );
            }

            throw upe;
        }
        catch ( MismatchingStoreIdException e )
        {
            userLog.info(
                    "The store does not represent the same database as master. Will remove and fetch a new one from " +
                    "master" );
            if ( txIdStore.getLastCommittedTransactionId() == BASE_TX_ID )
            {
                msgLog.warn( "Found and deleting empty store with mismatching store id", e );
                stopServicesAndHandleBranchedStore( BranchedDataPolicy.keep_none, masterUri, me, cancellationRequest );
                throw e;
            }

            msgLog.error( "Store cannot participate in cluster due to mismatching store IDs", e );
            throw new ForeignStoreException( e.getExpected(), e.getEncountered() );
        }
    }

    void stopServicesAndHandleBranchedStore( BranchedDataPolicy branchPolicy, URI masterUri, URI me,
                                             CancellationRequest cancellationRequest ) throws Throwable
    {
        MoveAfterCopy moveWithCopyThenBranch = ( moves, fromDirectory, toDirectory ) ->
        {
            stopServices();

            msgLog.debug( "Branching store: " + storeDir );
            branchPolicy.handle( storeDir, pageCache, logService );

            msgLog.debug( "Moving downloaded store from " + fromDirectory + " to " + toDirectory );
            MoveAfterCopy.moveReplaceExisting().move( moves, fromDirectory, toDirectory );
            msgLog.debug( "Moved downloaded store from " + fromDirectory + " to " + toDirectory );
        };
        copyStore( masterUri, me, cancellationRequest, moveWithCopyThenBranch );
    }

    private void copyStore( URI masterUri, URI me, CancellationRequest cancellationRequest,
                            MoveAfterCopy moveAfterCopy ) throws Throwable
    {
        boolean success = false;
        monitor.storeCopyStarted();
        LifeSupport copyLife = new LifeSupport();
        try
        {
            MasterClient masterClient = newMasterClient( masterUri, me, null, copyLife );
            copyLife.start();

            boolean masterIsOld = MasterClient.CURRENT.compareTo( masterClient.getProtocolVersion() ) > 0;
            if ( masterIsOld )
            {
                throw new UnableToCopyStoreFromOldMasterException( MasterClient.CURRENT.getApplicationProtocol(),
                        masterClient.getProtocolVersion().getApplicationProtocol() );
            }
            else
            {
                copyStoreFromMaster( masterClient, cancellationRequest, moveAfterCopy );
                success = true;
            }
        }
        finally
        {
            monitor.storeCopyCompleted( success );
            copyLife.shutdown();
        }
    }
}
