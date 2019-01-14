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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.io.file.Files.createOrOpenAsOutputStream;

public abstract class AbstractInProcessServerBuilder implements TestServerBuilder
{
    private File serverFolder;
    private final Extensions extensions = new Extensions();
    private final HarnessRegisteredProcs procedures = new HarnessRegisteredProcs();
    private final Fixtures fixtures = new Fixtures();

    /**
     * Config options for both database and server.
     */
    private final Map<String,String> config = new HashMap<>();

    public AbstractInProcessServerBuilder( File workingDir )
    {
        File dataDir = new File( workingDir, randomFolderName() ).getAbsoluteFile();
        init( dataDir );
    }

    public AbstractInProcessServerBuilder( File workingDir, String dataSubDir )
    {
        File dataDir = new File( workingDir, dataSubDir ).getAbsoluteFile();
        init( dataDir );
    }

    private void init( File workingDir )
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

    @Override
    public TestServerBuilder copyFrom( File originalStoreDir )
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
    public ServerControls newServer()
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

            config.put( ServerSettings.third_party_packages.name(), toStringForThirdPartyPackageProperty( extensions.toList() ) );
            config.put( GraphDatabaseSettings.store_internal_log_path.name(), internalLogFile.getAbsolutePath() );

            final FormattedLogProvider userLogProvider = FormattedLogProvider.toOutputStream( logOutputStream );
            GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
            Iterable<KernelExtensionFactory<?>> kernelExtensions =
                    append( new Neo4jHarnessExtensions( procedures ), dependencies.kernelExtensions() );
            dependencies = dependencies.kernelExtensions( kernelExtensions ).userLogProvider( userLogProvider );

            AbstractNeoServer neoServer = createNeoServer( config, dependencies, userLogProvider );
            InProcessServerControls controls = new InProcessServerControls( serverFolder, userLogFile, internalLogFile, neoServer, logOutputStream );
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

    protected abstract AbstractNeoServer createNeoServer( Map<String,String> config,
            GraphDatabaseFacadeFactory.Dependencies dependencies, FormattedLogProvider userLogProvider );

    @Override
    public TestServerBuilder withConfig( Setting<?> key, String value )
    {
        return withConfig( key.name(), value );
    }

    @Override
    public TestServerBuilder withConfig( String key, String value )
    {
        config.put( key, value );
        return this;
    }

    @Override
    public TestServerBuilder withExtension( String mountPath, Class<?> extension )
    {
        return withExtension( mountPath, extension.getPackage().getName() );
    }

    @Override
    public TestServerBuilder withExtension( String mountPath, String packageName )
    {
        extensions.add( mountPath, packageName );
        return this;
    }

    @Override
    public TestServerBuilder withFixture( File cypherFileOrDirectory )
    {
        fixtures.add( cypherFileOrDirectory );
        return this;
    }

    @Override
    public TestServerBuilder withFixture( String fixtureStatement )
    {
        fixtures.add( fixtureStatement );
        return this;
    }

    @Override
    public TestServerBuilder withFixture( Function<GraphDatabaseService,Void> fixtureFunction )
    {
        fixtures.add( fixtureFunction );
        return this;
    }

    @Override
    public TestServerBuilder withProcedure( Class<?> procedureClass )
    {
        procedures.addProcedure( procedureClass );
        return this;
    }

    @Override
    public TestServerBuilder withFunction( Class<?> functionClass )
    {
        procedures.addFunction( functionClass );
        return this;
    }

    @Override
    public TestServerBuilder withAggregationFunction( Class<?> functionClass )
    {
        procedures.addAggregationFunction( functionClass );
        return this;
    }

    private TestServerBuilder setDirectory( File dir )
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
        String propertyString = "";
        int packageCount = extensions.size();

        if ( packageCount == 0 )
        {
            return propertyString;
        }
        else
        {
            ThirdPartyJaxRsPackage jaxRsPackage;
            for ( int i = 0; i < packageCount - 1; i++ )
            {
                jaxRsPackage = extensions.get( i );
                propertyString += jaxRsPackage.getPackageName() + "=" + jaxRsPackage.getMountPoint() + Settings.SEPARATOR;
            }
            jaxRsPackage = extensions.get( packageCount - 1 );
            propertyString += jaxRsPackage.getPackageName() + "=" + jaxRsPackage.getMountPoint();
            return propertyString;
        }
    }

    /**
     * A kernel extension used to ensure we load user-registered procedures
     * after other kernel extensions have initialized, since kernel extensions
     * can add custom injectables that procedures need.
     */
    private static class Neo4jHarnessExtensions extends KernelExtensionFactory<Neo4jHarnessExtensions.Dependencies>
    {
        interface Dependencies
        {
            Procedures procedures();
        }

        private HarnessRegisteredProcs userProcs;

        Neo4jHarnessExtensions( HarnessRegisteredProcs userProcs )
        {
            super( "harness" );
            this.userProcs = userProcs;
        }

        @Override
        public Lifecycle newInstance( KernelContext context,
                Dependencies dependencies )
        {
            return new LifecycleAdapter()
            {
                @Override
                public void start() throws Throwable
                {
                    userProcs.applyTo( dependencies.procedures() );
                }
            };
        }

    }
}
