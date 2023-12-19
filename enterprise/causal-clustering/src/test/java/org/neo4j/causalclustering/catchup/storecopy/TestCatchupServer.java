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
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupProtocolServerInstaller;
import org.neo4j.causalclustering.catchup.CheckPointerService;
import org.neo4j.causalclustering.catchup.RegularCatchupServerHandler;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.net.ChildInitializer;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;

class TestCatchupServer extends Server
{
    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();

    TestCatchupServer( FileSystemAbstraction fileSystem, GraphDatabaseAPI graphDb )
    {
        super( childInitializer( fileSystem, graphDb ), LOG_PROVIDER, LOG_PROVIDER, new ListenSocketAddress( "localhost", PortAuthority.allocatePort() ),
                "fake-catchup-server" );
    }

    private static ChildInitializer childInitializer( FileSystemAbstraction fileSystem, GraphDatabaseAPI graphDb )
    {
        ApplicationSupportedProtocols catchupProtocols = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
        ModifierSupportedProtocols modifierProtocols = new ModifierSupportedProtocols( COMPRESSION, emptyList() );

        ApplicationProtocolRepository catchupRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
        ModifierProtocolRepository modifierRepository = new ModifierProtocolRepository( ModifierProtocols.values(), singletonList( modifierProtocols ) );

        DependencyResolver dependencies = graphDb.getDependencyResolver();
        StoreCopyCheckPointMutex storeCopyCheckPointMutex = dependencies.resolveDependency( StoreCopyCheckPointMutex.class );
        Supplier<CheckPointer> checkPointer = () -> graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
        BooleanSupplier availability = () -> graphDb.getDependencyResolver().resolveDependency( AvailabilityGuard.class ).isAvailable();
        Supplier<NeoStoreDataSource> dataSource = () -> graphDb.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );
        PageCache pageCache = graphDb.getDependencyResolver().resolveDependency( PageCache.class );
        LogProvider logProvider = NullLogProvider.getInstance();

        org.neo4j.kernel.impl.store.StoreId kernelStoreId = dataSource.get().getStoreId();
        StoreId storeId = new StoreId( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId(), kernelStoreId.getUpgradeTime(),
                kernelStoreId.getUpgradeId() );

        RegularCatchupServerHandler catchupServerHandler = new RegularCatchupServerHandler( new Monitors(), logProvider,
                () -> storeId, dependencies.provideDependency( TransactionIdStore.class ), dependencies.provideDependency( LogicalTransactionStore.class ),
                dataSource, availability, fileSystem, pageCache, storeCopyCheckPointMutex, null,
                new CheckPointerService( checkPointer, new CentralJobScheduler(), JobScheduler.Groups.checkPoint ) );

        NettyPipelineBuilderFactory pipelineBuilder = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );
        CatchupProtocolServerInstaller.Factory catchupProtocolServerInstaller = new CatchupProtocolServerInstaller.Factory( pipelineBuilder, logProvider,
                catchupServerHandler );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                singletonList( catchupProtocolServerInstaller ), ModifierProtocolInstaller.allServerInstallers );

        return new HandshakeServerInitializer( catchupRepository, modifierRepository, protocolInstallerRepository, pipelineBuilder, logProvider );
    }
}
