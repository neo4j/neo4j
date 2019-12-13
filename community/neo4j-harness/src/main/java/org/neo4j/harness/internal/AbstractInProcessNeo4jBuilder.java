/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.harness.internal;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.test.ssl.SelfSignedCertificateFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;
import static org.neo4j.internal.helpers.collection.Iterables.addAll;
import static org.neo4j.internal.helpers.collection.Iterables.append;
import static org.neo4j.io.fs.FileSystemUtils.createOrOpenAsOutputStream;

public abstract class AbstractInProcessNeo4jBuilder implements Neo4jBuilder
{
    private File serverFolder;
    private final Extensions unmanagedExtentions = new Extensions();
    private final HarnessRegisteredProcs procedures = new HarnessRegisteredProcs();
    private final Fixtures fixtures = new Fixtures();
    private final List<ExtensionFactory<?>> extensionFactories = new ArrayList<>();
    private boolean disabledServer;
    private final Config.Builder config = Config.newBuilder();

    public AbstractInProcessNeo4jBuilder()
    {
    }

    public AbstractInProcessNeo4jBuilder( File workingDir, String dataSubDir )
    {
        File dataDir = new File( workingDir, dataSubDir ).getAbsoluteFile();
        withWorkingDir( dataDir );
    }

    @Override
    public Neo4jBuilder withWorkingDir( File workingDirectory )
    {
        File dataDir = new File( workingDirectory, randomFolderName() ).getAbsoluteFile();
        setWorkingDirectory( dataDir );
        return this;
    }

    @Override
    public Neo4jBuilder copyFrom( File originalStoreDir )
    {
        try
        {
            FileUtils.copyDirectory( originalStoreDir, serverFolder );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return this;
    }

    @Override
    public InProcessNeo4j build()
    {
        File userLogFile = new File( serverFolder, "neo4j.log" );
        File internalLogFile = new File( serverFolder, "debug.log" );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              OutputStream userLogOutputStream = openStream( fileSystem, userLogFile ) )
        {
            config.set( ServerSettings.third_party_packages, unmanagedExtentions.toList() );
            config.set( GraphDatabaseSettings.store_internal_log_path, internalLogFile.toPath().toAbsolutePath() );

            var certificates = new File( serverFolder, "certificates" );
            if ( disabledServer )
            {
                config.set( HttpConnector.enabled, false );
                config.set( HttpsConnector.enabled, false );
            }

            Config dbConfig = config.build();
            if ( dbConfig.get( HttpsConnector.enabled ) ||
                 dbConfig.get( BoltConnector.enabled ) && dbConfig.get( BoltConnector.encryption_level ) != BoltConnector.EncryptionLevel.DISABLED )
            {
                SelfSignedCertificateFactory.create( certificates );
                List<SslPolicyConfig> policies = List.of( SslPolicyConfig.forScope( HTTPS ), SslPolicyConfig.forScope( BOLT ) );
                for ( SslPolicyConfig policy : policies )
                {
                    config.set( policy.enabled, Boolean.TRUE );
                    config.set( policy.base_directory, certificates.toPath() );
                    config.set( policy.trust_all, true );
                    config.set( policy.client_auth, ClientAuth.NONE );
                }
                dbConfig = config.build();
            }

            LogProvider userLogProvider = FormattedLogProvider.withZoneId( dbConfig.get( db_timezone ).getZoneId() ).toOutputStream( userLogOutputStream );
            GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies().userLogProvider( userLogProvider );
            dependencies = dependencies.extensions( buildExtensionList( dependencies ) );

            var managementService = createNeo( dbConfig, dependencies );

            InProcessNeo4j controls = new InProcessNeo4j( serverFolder, userLogFile, internalLogFile, managementService, dbConfig, userLogOutputStream );
            controls.start();

            try
            {
                fixtures.applyTo( controls );
            }
            catch ( Exception e )
            {
                controls.close();
                throw e;
            }
            return controls;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected abstract DatabaseManagementService createNeo( Config config, ExternalDependencies dependencies );

    @Override
    public <T> Neo4jBuilder withConfig( Setting<T> setting, T value )
    {
        config.set( setting, value );
        return this;
    }

    @Override
    public Neo4jBuilder withUnmanagedExtension( String mountPath, Class<?> extension )
    {
        return withUnmanagedExtension( mountPath, extension.getPackage().getName() );
    }

    @Override
    public Neo4jBuilder withUnmanagedExtension( String mountPath, String packageName )
    {
        unmanagedExtentions.add( mountPath, packageName );
        return this;
    }

    @Override
    public Neo4jBuilder withExtensionFactories( Iterable<ExtensionFactory<?>> extensionFactories )
    {
        addAll(this.extensionFactories, extensionFactories);
        return this;
    }

    @Override
    public Neo4jBuilder withDisabledServer()
    {
        this.disabledServer = true;
        return this;
    }

    @Override
    public Neo4jBuilder withFixture( File cypherFileOrDirectory )
    {
        fixtures.add( cypherFileOrDirectory );
        return this;
    }

    @Override
    public Neo4jBuilder withFixture( String fixtureStatement )
    {
        fixtures.add( fixtureStatement );
        return this;
    }

    @Override
    public Neo4jBuilder withFixture( Function<GraphDatabaseService,Void> fixtureFunction )
    {
        fixtures.add( fixtureFunction );
        return this;
    }

    @Override
    public Neo4jBuilder withProcedure( Class<?> procedureClass )
    {
        procedures.addProcedure( procedureClass );
        return this;
    }

    @Override
    public Neo4jBuilder withFunction( Class<?> functionClass )
    {
        procedures.addFunction( functionClass );
        return this;
    }

    @Override
    public Neo4jBuilder withAggregationFunction( Class<?> functionClass )
    {
        procedures.addAggregationFunction( functionClass );
        return this;
    }

    private Iterable<ExtensionFactory<?>> buildExtensionList( GraphDatabaseDependencies dependencies )
    {
        Iterable<ExtensionFactory<?>> extensions = append( new Neo4jHarnessExtensions( procedures ), dependencies.extensions() );
        return addAll( this.extensionFactories, extensions );
    }

    private void setWorkingDirectory( File workingDir )
    {
        setDirectory( workingDir );
        withConfig( auth_enabled, false );
        withConfig( pagecache_memory, "8m" );

        withConfig( HttpConnector.enabled, true );
        withConfig( HttpConnector.listen_address, new SocketAddress( "localhost", 0 ) );

        withConfig( HttpsConnector.enabled, false );
        withConfig( HttpsConnector.listen_address, new SocketAddress( "localhost", 0 ) );

        withConfig( BoltConnector.enabled, true );
        withConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
    }

    private Neo4jBuilder setDirectory( File dir )
    {
        this.serverFolder = dir;
        config.set( neo4j_home, serverFolder.toPath().toAbsolutePath() );
        return this;
    }

    private String randomFolderName()
    {
        return DigestUtils.md5Hex( Long.toString( ThreadLocalRandom.current().nextLong() ) );
    }

    private static StringBuilder describeJaxRsPackage( StringBuilder builder, ThirdPartyJaxRsPackage jaxRsPackage )
    {
        return builder.append( jaxRsPackage.getPackageName() ).append( "=" ).append( jaxRsPackage.getMountPoint() );
    }

    private static OutputStream openStream( FileSystemAbstraction fs, File file )
    {
        try
        {
            return createOrOpenAsOutputStream( fs, file, true );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to create log file", e );
        }
    }

    /**
     * A kernel extension used to ensure we load user-registered procedures
     * after other kernel extensions have initialized, since kernel extensions
     * can add custom injectables that procedures need.
     */
    private static class Neo4jHarnessExtensions extends ExtensionFactory<Neo4jHarnessExtensions.Dependencies>
    {
        interface Dependencies
        {
            GlobalProcedures procedures();
        }

        private HarnessRegisteredProcs userProcs;

        Neo4jHarnessExtensions( HarnessRegisteredProcs userProcs )
        {
            super( "harness" );
            this.userProcs = userProcs;
        }

        @Override
        public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
        {
            return new LifecycleAdapter()
            {
                @Override
                public void start() throws Exception
                {
                    userProcs.applyTo( dependencies.procedures() );
                }
            };
        }
    }
}
