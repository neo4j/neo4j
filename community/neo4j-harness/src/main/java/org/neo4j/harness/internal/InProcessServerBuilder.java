/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.InternalAbstractGraphDatabase.Dependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.ClassicLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.configuration.Configurator.DATABASE_LOCATION_PROPERTY_KEY;
import static org.neo4j.server.configuration.Configurator.WEBSERVER_PORT_PROPERTY_KEY;
import static org.neo4j.test.Digests.md5Hex;

public class InProcessServerBuilder implements TestServerBuilder
{
    private File serverFolder;
    private Logging logging;
    private final Extensions extensions = new Extensions();
    private final Fixtures fixtures = new Fixtures();

    /**
     * Config options for both database and server.
     */
    private final Map<String, String> config = new HashMap<>();

    public InProcessServerBuilder( File workingDir )
    {
        setDirectory( workingDir );
        withConfig( ServerSettings.auth_enabled, "false" );
        withConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        withConfig( WEBSERVER_PORT_PROPERTY_KEY, Integer.toString( freePort() ) );
    }

    @Override
    public ServerControls newServer()
    {
        Dependencies dependencies = GraphDatabaseDependencies.newDependencies().logging( logging );
        InProcessServerControls controls = new InProcessServerControls( serverFolder,
                new CommunityNeoServer( new MapConfigurator( config, extensions.toList() ), dependencies ), logging );
        controls.start();
        try
        {
            fixtures.applyTo( controls.httpURI() );
        }
        catch(RuntimeException e)
        {
            controls.close();
            throw e;
        }
        return controls;
    }

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
        extensions.add(mountPath, packageName);
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

    private TestServerBuilder setDirectory( File dir )
    {
        this.serverFolder = new File(dir, randomFolderName()).getAbsoluteFile();
        config.put( DATABASE_LOCATION_PROPERTY_KEY, serverFolder.getAbsolutePath() );
        logging = new ClassicLoggingService( new Config( stringMap( store_dir.name(), serverFolder.getAbsolutePath() ) ) );
        return this;
    }

    private String randomFolderName()
    {
        return md5Hex( Long.toString( new Random().nextLong() ) );
    }

    private int freePort()
    {
        try
        {
            return Ports.findFreePort( Ports.INADDR_LOCALHOST, new int[]{7474, 10000} ).getPort();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to find an available port: " + e.getMessage(), e );
        }
    }
}
