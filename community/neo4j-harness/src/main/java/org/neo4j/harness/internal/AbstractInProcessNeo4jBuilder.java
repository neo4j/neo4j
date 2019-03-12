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
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpConnector.Encryption;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.LogTimeZone;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.DisabledNeoServer;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.database.GraphFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.Iterables.addAll;
import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.io.fs.FileSystemUtils.createOrOpenAsOutputStream;

public abstract class AbstractInProcessNeo4jBuilder implements Neo4jBuilder
{
    private File serverFolder;
    private final Extensions unmanagedExtentions = new Extensions();
    private final HarnessRegisteredProcs procedures = new HarnessRegisteredProcs();
    private final Fixtures fixtures = new Fixtures();
    private final List<ExtensionFactory<?>> extensionFactories = new ArrayList<>();
    private boolean disabledServer;
    private final Map<String,String> config = new HashMap<>();

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
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            File userLogFile = new File( serverFolder, "neo4j.log" );
            File internalLogFile = new File( serverFolder, "debug.log" );

            final OutputStream logOutputStream;
            try
            {
                logOutputStream = createOrOpenAsOutputStream( fileSystem, userLogFile, true );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to create log file", e );
            }

            config.put( ServerSettings.third_party_packages.name(), toStringForThirdPartyPackageProperty( unmanagedExtentions.toList() ) );
            config.put( GraphDatabaseSettings.store_internal_log_path.name(), internalLogFile.getAbsolutePath() );

            if ( disabledServer )
            {
                config.put( "dbms.connector.http.enabled", Settings.FALSE );
                config.put( "dbms.connector.https.enabled", Settings.FALSE );
            }

            LogProvider userLogProvider = FormattedLogProvider.withZoneId( logZoneIdFrom( config ) ).toOutputStream( logOutputStream );
            GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies().userLogProvider( userLogProvider );
            dependencies = dependencies.extensions( buildExtensionList( dependencies ) );

            Config dbConfig = Config.defaults( config );
            GraphFactory graphFactory = createGraphFactory( dbConfig );
            boolean httpAndHttpsDisabled = dbConfig.enabledHttpConnectors().isEmpty();

            NeoServer server = startNeo4jServer( dependencies, dbConfig, graphFactory, httpAndHttpsDisabled );

            InProcessNeo4j controls = new InProcessNeo4j( serverFolder, userLogFile, internalLogFile, server, logOutputStream );
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

    protected abstract GraphFactory createGraphFactory( Config config );

    protected abstract AbstractNeoServer createNeoServer( GraphFactory graphFactory, Config config, ExternalDependencies dependencies );

    @Override
    public Neo4jBuilder withConfig( Setting<?> key, String value )
    {
        return withConfig( key.name(), value );
    }

    @Override
    public Neo4jBuilder withConfig( String key, String value )
    {
        config.put( key, value );
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

    private NeoServer startNeo4jServer( GraphDatabaseDependencies dependencies, Config dbConfig, GraphFactory graphFactory, boolean httpAndHttpsDisabled )
    {
        return httpAndHttpsDisabled ? new DisabledNeoServer( graphFactory, dependencies, dbConfig ) : createNeoServer( graphFactory, dbConfig, dependencies );
    }

    private Iterable<ExtensionFactory<?>> buildExtensionList( GraphDatabaseDependencies dependencies )
    {
        Iterable<ExtensionFactory<?>> extensions = append( new Neo4jHarnessExtensions( procedures ), dependencies.extensions() );
        return addAll( this.extensionFactories, extensions );
    }

    private void setWorkingDirectory( File workingDir )
    {
        setDirectory( workingDir );
        withConfig( auth_enabled, "false" );
        withConfig( pagecache_memory, "8m" );

        BoltConnector bolt0 = new BoltConnector( "bolt" );
        HttpConnector http1 = new HttpConnector( "http", Encryption.NONE );
        HttpConnector http2 = new HttpConnector( "https", Encryption.TLS );

        withConfig( http1.type, "HTTP" );
        withConfig( http1.encryption, Encryption.NONE.name() );
        withConfig( http1.enabled, "true" );
        withConfig( http1.address, "localhost:0" );

        withConfig( http2.type, "HTTP" );
        withConfig( http2.encryption, Encryption.TLS.name() );
        withConfig( http2.enabled, "false" );
        withConfig( http2.address, "localhost:0" );

        withConfig( bolt0.type, "BOLT" );
        withConfig( bolt0.enabled, "true" );
        withConfig( bolt0.address, "localhost:0" );
    }

    private Neo4jBuilder setDirectory( File dir )
    {
        this.serverFolder = dir;
        config.put( data_directory.name(), serverFolder.getAbsolutePath() );
        return this;
    }

    private String randomFolderName()
    {
        return DigestUtils.md5Hex( Long.toString( ThreadLocalRandom.current().nextLong() ) );
    }

    private static String toStringForThirdPartyPackageProperty( List<ThirdPartyJaxRsPackage> extensions )
    {
        int packageCount = extensions.size();
        if ( packageCount == 0 )
        {
            return StringUtils.EMPTY;
        }
        StringBuilder builder = new StringBuilder();
        ThirdPartyJaxRsPackage jaxRsPackage;
        for ( int i = 0; i < packageCount - 1; i++ )
        {
            jaxRsPackage = extensions.get( i );
            describeJaxRsPackage( builder, jaxRsPackage ).append( Settings.SEPARATOR );
        }
        jaxRsPackage = extensions.get( packageCount - 1 );
        describeJaxRsPackage( builder, jaxRsPackage );
        return builder.toString();
    }

    private static StringBuilder describeJaxRsPackage( StringBuilder builder, ThirdPartyJaxRsPackage jaxRsPackage )
    {
        return builder.append( jaxRsPackage.getPackageName() ).append( "=" ).append( jaxRsPackage.getMountPoint() );
    }

    private static ZoneId logZoneIdFrom( Map<String,String> config )
    {
        String dbTimeZone = config.getOrDefault( db_timezone.name(), db_timezone.getDefaultValue() );
        return LogTimeZone.valueOf( dbTimeZone ).getZoneId();
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
