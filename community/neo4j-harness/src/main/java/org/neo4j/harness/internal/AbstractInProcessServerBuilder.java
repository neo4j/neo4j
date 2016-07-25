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
package org.neo4j.harness.internal;

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
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.helpers.Exceptions;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
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

import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;
import static org.neo4j.server.configuration.ServerSettings.httpConnector;
import static org.neo4j.test.Digests.md5Hex;

public abstract class AbstractInProcessServerBuilder implements TestServerBuilder
{
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
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

    private void init( File workingDir )
    {
        setDirectory( workingDir );
        withConfig( auth_enabled, "false" );
        withConfig( pagecache_memory, "8m" );
        withConfig( httpConnector( "1" ).type, "HTTP" );
        withConfig( httpConnector( "1" ).enabled, "true" );
        withConfig( httpConnector( "1" ).address, "localhost:" + Integer.toString( freePort( 1001, 5000 ) ) );
        withConfig( boltConnector( "0" ).type, "BOLT" );
        withConfig( boltConnector( "0" ).enabled, "true" );
        withConfig( boltConnector( "0" ).address, "localhost:" + Integer.toString( freePort( 5001, 9000 ) ) );
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
        final OutputStream logOutputStream;
        try
        {
            logOutputStream = createOrOpenAsOuputStream( fileSystem, new File( serverFolder, "neo4j.log" ), true );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to create log file", e );
        }

        config.put( ServerSettings.third_party_packages.name(),
                toStringForThirdPartyPackageProperty( extensions.toList() ) );

        final FormattedLogProvider userLogProvider = FormattedLogProvider.toOutputStream( logOutputStream );
        GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
        dependencies = dependencies.kernelExtensions(
                append( new Neo4jHarnessExtensions( procedures ), dependencies.kernelExtensions() ) )
                .userLogProvider( userLogProvider );

        AbstractNeoServer neoServer = createNeoServer( config, dependencies, userLogProvider );
        InProcessServerControls controls = new InProcessServerControls( serverFolder, neoServer, logOutputStream );
        controls.start();

        try
        {
            fixtures.applyTo( controls );
        }
        catch ( Exception e )
        {
            controls.close();
            throw Exceptions.launderedException( e );
        }
        return controls;
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
        procedures.add( procedureClass );
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
        return md5Hex( Long.toString( ThreadLocalRandom.current().nextLong() ) );
    }

    private int freePort(int startRange, int endRange)
    {
        try
        {
            return Ports.findFreePort( Ports.INADDR_LOCALHOST, new int[]{startRange, endRange} ).getPort();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to find an available port: " + e.getMessage(), e );
        }
    }

    private static String toStringForThirdPartyPackageProperty( List<ThirdPartyJaxRsPackage> extensions )
    {
        String propertyString = "";
        int packageCount = extensions.size();

        if( packageCount == 0 )
            return propertyString;
        else
        {
            ThirdPartyJaxRsPackage jaxRsPackage;
            for( int i = 0; i < packageCount - 1; i ++ )
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

        public Neo4jHarnessExtensions( HarnessRegisteredProcs userProcs )
        {
            super("harness");
            this.userProcs = userProcs;
        }

        @Override
        public Lifecycle newInstance( KernelContext context,
                Dependencies dependencies ) throws Throwable
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
