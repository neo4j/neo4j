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

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.List;
import java.util.function.BiFunction;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.bouncycastle.operator.OperatorCreationException;

import org.neo4j.bolt.security.ssl.Certificates;
import org.neo4j.bolt.security.ssl.KeyStoreFactory;
import org.neo4j.bolt.security.ssl.KeyStoreInformation;
import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.transport.Netty4LogBridge;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.NettyServer.ProtocolInitializer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.bolt.v1.runtime.MonitoredSessions;
import org.neo4j.bolt.v1.runtime.Sessions;
import org.neo4j.bolt.v1.runtime.internal.EncryptionRequiredSessions;
import org.neo4j.bolt.v1.runtime.internal.StandardSessions;
import org.neo4j.bolt.v1.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.bolt.v1.transport.BoltProtocolV1;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.Description;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.udc.UsageData;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import static org.neo4j.collection.primitive.Primitive.longObjectMap;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.GroupSettingSupport.enumerate;
import static org.neo4j.kernel.configuration.Settings.*;
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
        public static Setting<File> tls_certificate_file = derivedSetting(
                "unsupported.dbms.security.tls_certificate_file", certificates_directory,
                ( certificates ) -> new File( certificates, "neo4j.cert" ), PATH );

        @Internal
        @Description( "Path to the X.509 private key to be used by Neo4j for TLS connections" )
        public static final Setting<File> tls_key_file = derivedSetting(
                "unsupported.dbms.security.tls_key_file", certificates_directory,
                (certificates ) -> new File( certificates, "neo4j.key" ), PATH );
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
        final GraphDatabaseFacade api = (GraphDatabaseFacade) gdb;
        final LogService logging = dependencies.logService();
        final Log log = logging.getInternalLog( Sessions.class );

        final LifeSupport life = new LifeSupport();

        final JobScheduler scheduler = dependencies.scheduler();

        Netty4LogBridge.setLogProvider( logging.getInternalLogProvider() );

        Sessions sessions =
                new MonitoredSessions( dependencies.monitors(),
                        new ThreadedSessions(
                                life.add( new StandardSessions( api, dependencies.usageData(), logging,
                                        dependencies.txBridge() ) ),
                                scheduler, logging ), Clock.systemUTC() );

        List<ProtocolInitializer> connectors = config
                .view( enumerate( GraphDatabaseSettings.Connector.class ) )
                .map( BoltConnector::new )
                .filter( ( connConfig ) -> BOLT.equals( config.get( connConfig.type ) )
                        && config.get( connConfig.enabled ) )
                .map( ( connConfig ) -> {
                    HostnamePort address = config.get( connConfig.address );
                    SslContext sslCtx;
                    boolean requireEncryption = false;
                    switch ( config.get( connConfig.encryption_level ) )
                    {
                        // self signed cert should be generated when encryption is REQUIRED or OPTIONAL on the server
                        // while no cert is generated if encryption is DISABLED
                        case REQUIRED:
                            requireEncryption = true;
                            // no break here
                        case OPTIONAL:
                            sslCtx = createSslContext( config, log, address );
                            break;
                        default:
                            // case DISABLED:
                            sslCtx = null;
                            break;
                    }

                    return new SocketTransport( address, sslCtx, logging.getInternalLogProvider(),
                            newVersions( logging, requireEncryption ?
                                    new EncryptionRequiredSessions( sessions ) : sessions ) );
                } )
                .collect( toList() );

        if ( connectors.size() > 0 && !config.get( GraphDatabaseSettings.disconnected ) )
        {
            life.add( new NettyServer( scheduler.threadFactory( boltNetworkIO ), connectors ) );
            log.info( "Bolt Server extension loaded." );
            for ( ProtocolInitializer connector : connectors )
            {
                logging.getUserLog( Sessions.class ).info( "Bolt enabled on %s.", connector.address() );
            }
        }

        return life;
    }

    private SslContext createSslContext( Config config, Log log, HostnamePort address )
    {
        try
        {
            KeyStoreInformation keyStore = createKeyStore( config, log, address );
            return SslContextBuilder
                    .forServer( keyStore.getCertificatePath(), keyStore.getPrivateKeyPath() )
                    .build();
        }
        catch(IOException | OperatorCreationException | GeneralSecurityException e )
        {
            throw new RuntimeException( "Failed to initilize SSL encryption support, which is required to start this " +
                                        "connector. Error was: " + e.getMessage(), e );
        }
    }

    private PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> newVersions( LogService logging,
            Sessions sessions )
    {
        PrimitiveLongObjectMap<BiFunction<Channel,Boolean,BoltProtocol>> availableVersions = longObjectMap();
        availableVersions.put(
                BoltProtocolV1.VERSION,
                ( channel, isEncrypted ) -> {
                    String descriptor = format( "\tclient%s\tserver%s", channel.remoteAddress(), channel.localAddress() );
                    return new BoltProtocolV1( logging, sessions.newSession( descriptor, isEncrypted ), channel );
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
                            "certificate.",
                            certificatePath ) );
        }
        if ( !privateKeyPath.exists() )
        {
            throw new IllegalStateException(
                    format( "TLS certificate found, but missing key at '%s'. Cannot start server without key.",
                            privateKeyPath ) );
        }

        return new KeyStoreFactory().createKeyStore( privateKeyPath, certificatePath );
    }
}
