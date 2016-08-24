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
package org.neo4j.bolt;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.security.ssl.Certificates;
import org.neo4j.bolt.security.ssl.KeyStoreFactory;
import org.neo4j.bolt.security.ssl.KeyStoreInformation;
import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.transport.Netty4LogBridge;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.NettyServer.ProtocolInitializer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.LifecycleManagedBoltFactory;
import org.neo4j.bolt.v1.runtime.MonitoredWorkerFactory;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.bolt.v1.runtime.concurrent.ThreadedWorkerFactory;
import org.neo4j.bolt.v1.transport.BoltProtocolV1;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.GroupSettingSupport.enumerate;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.pathSetting;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.boltNetworkIO;

/**
 * Wraps Bolt and exposes it as a Kernel Extension.
 */
@Service.Implementation( KernelExtensionFactory.class )
public class BoltKernelExtension extends KernelExtensionFactory<BoltKernelExtension.Dependencies>
{
    public static class Settings
    {
        @Description( "Directory for storing certificates to be used by Neo4j for TLS connections" )
        public static Setting<File> certificates_directory =
                pathSetting( "dbms.directories.certificates", "certificates" );

        @Internal
        @Description( "Path to the X.509 public certificate to be used by Neo4j for TLS connections" )
        public static Setting<File> tls_certificate_file =
                derivedSetting( "unsupported.dbms.security.tls_certificate_file", certificates_directory,
                        ( certificates ) -> new File( certificates, "neo4j.cert" ), PATH );

        @Internal
        @Description( "Path to the X.509 private key to be used by Neo4j for TLS connections" )
        public static final Setting<File> tls_key_file =
                derivedSetting( "unsupported.dbms.security.tls_key_file", certificates_directory,
                        ( certificates ) -> new File( certificates, "neo4j.key" ), PATH );
    }

    public interface Dependencies
    {
        LogService logService();

        Config config();

        GraphDatabaseService db();

        JobScheduler scheduler();

        UsageData usageData();

        Monitors monitors();

        ThreadToStatementContextBridge txBridge();

        BoltConnectionTracker sessionTracker();

        NeoStoreDataSource dataSource();

        AuthManager authManager();
    }

    public BoltKernelExtension()
    {
        super( "bolt-server" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        final Config config = dependencies.config();
        final GraphDatabaseService gdb = dependencies.db();
        final GraphDatabaseAPI api = (GraphDatabaseAPI) gdb;
        final LogService logService = dependencies.logService();
        final Log log = logService.getInternalLog( WorkerFactory.class );

        final LifeSupport life = new LifeSupport();

        final JobScheduler scheduler = dependencies.scheduler();

        Netty4LogBridge.setLogProvider( logService.getInternalLogProvider() );

        Authentication authentication = authentication( dependencies.config(), dependencies.authManager(), logService );

        BoltFactory boltConnectionManagerFactory = life.add(
                new LifecycleManagedBoltFactory( api, dependencies.usageData(), logService, dependencies.txBridge(),
                        authentication, dependencies.sessionTracker() ) );
        ThreadedWorkerFactory threadedSessions = new ThreadedWorkerFactory( boltConnectionManagerFactory, scheduler, logService );
        WorkerFactory workerFactory = new MonitoredWorkerFactory( dependencies.monitors(), threadedSessions,
                Clocks.systemClock() );

        List<ProtocolInitializer> connectors = config
                .view( enumerate( GraphDatabaseSettings.Connector.class ) )
                .map( BoltConnector::new )
                .filter( ( connConfig ) -> BOLT.equals( config.get( connConfig.type ) )
                        && config.get( connConfig.enabled ) )
                .map( ( connConfig ) -> {
                    HostnamePort address = config.get( connConfig.address );
                    SslContext sslCtx;
                    boolean requireEncryption;
                    final BoltConnector.EncryptionLevel encryptionLevel = config.get( connConfig.encryption_level );
                    switch ( encryptionLevel )
                    {
                    case REQUIRED:
                        // Encrypted connections are mandatory, a self-signed certificate may be generated.
                        requireEncryption = true;
                        sslCtx = createSslContext( config, log, address );
                        break;
                    case OPTIONAL:
                        // Encrypted connections are optional, a self-signed certificate may be generated.
                        requireEncryption = false;
                        sslCtx = createSslContext( config, log, address );
                        break;
                    case DISABLED:
                        // Encryption is turned off, no self-signed certificate will be generated.
                        requireEncryption = false;
                        sslCtx = null;
                        break;
                    default:
                        // In the unlikely event that we happen to fall through to the default option here,
                        // there is a mismatch between the BoltConnector.EncryptionLevel enum and the options
                        // handled in this switch statement. In this case, we'll log a warning and default to
                        // disabling encryption, since this mirrors the functionality introduced in 3.0.
                        log.warn( format( "Unhandled encryption level %s - assuming DISABLED.", encryptionLevel.name() ) );
                        requireEncryption = false;
                        sslCtx = null;
                        break;
                    }

                    final Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> versions =
                            newVersions( logService, workerFactory );
                    return new SocketTransport( address, sslCtx, requireEncryption, logService.getInternalLogProvider(), versions );
                } )
                .collect( toList() );

        if ( connectors.size() > 0 && !config.get( GraphDatabaseSettings.disconnected ) )
        {
            life.add( new NettyServer( scheduler.threadFactory( boltNetworkIO ), connectors ) );
            log.info( "Bolt Server extension loaded." );
            for ( ProtocolInitializer connector : connectors )
            {
                logService.getUserLog( WorkerFactory.class ).info( "Bolt enabled on %s.", connector.address() );
            }
        }

        return life;
    }

    private SslContext createSslContext( Config config, Log log, HostnamePort address )
    {
        try
        {
            KeyStoreInformation keyStore = createKeyStore( config, log, address );
            return SslContextBuilder.forServer( keyStore.getCertificatePath(), keyStore.getPrivateKeyPath() ).build();
        }
        catch ( IOException | OperatorCreationException | GeneralSecurityException e )
        {
            throw new RuntimeException( "Failed to initialize SSL encryption support, which is required to start " +
                    "this connector. Error was: " + e.getMessage(), e );
        }
    }

    private Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> newVersions(
            LogService logging, WorkerFactory workerFactory )
    {
        Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> availableVersions = new HashMap<>();
        availableVersions.put(
                (long) BoltProtocolV1.VERSION,
                ( channel, isEncrypted ) -> {
                    String descriptor = format( "\tclient%s\tserver%s", channel.remoteAddress(), channel.localAddress() );
                    BoltWorker worker = workerFactory.newWorker( descriptor, channel::close );
                    return new BoltProtocolV1( worker, channel, logging );
                }
        );
        return availableVersions;
    }

    private KeyStoreInformation createKeyStore( Configuration config, Log log, HostnamePort address )
            throws GeneralSecurityException, IOException, OperatorCreationException
    {
        File privateKeyPath = config.get( Settings.tls_key_file ).getAbsoluteFile();
        File certificatePath = config.get( Settings.tls_certificate_file ).getAbsoluteFile();

        if ( (!certificatePath.exists() && !privateKeyPath.exists()) )
        {
            log.info( "No SSL certificate found, generating a self-signed certificate.." );
            Certificates certFactory = new Certificates();
            certFactory.createSelfSignedCertificate( certificatePath, privateKeyPath, address.getHost() );
        }

        if ( !certificatePath.exists() )
        {
            throw new IllegalStateException(
                    format( "TLS private key found, but missing certificate at '%s'. Cannot start server without " +
                            "certificate.", certificatePath ) );
        }
        if ( !privateKeyPath.exists() )
        {
            throw new IllegalStateException(
                    format( "TLS certificate found, but missing key at '%s'. Cannot start server without key.",
                            privateKeyPath ) );
        }

        return new KeyStoreFactory().createKeyStore( privateKeyPath, certificatePath );
    }

    private Authentication authentication( Config config, AuthManager authManager, LogService logService )
    {

        if ( config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            return new BasicAuthentication( authManager, logService.getInternalLogProvider() );
        }
        else
        {
            return Authentication.NONE;
        }
    }
}
