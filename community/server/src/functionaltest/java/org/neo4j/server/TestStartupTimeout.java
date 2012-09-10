/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.test.TargetDirectory;

public class TestStartupTimeout {

    TargetDirectory target = TargetDirectory.forTest( TestStartupTimeout.class );
    private static final String DIRSEP = File.separator;

    @Rule
    public TargetDirectory.TestDirectory test = target.cleanTestDirectory();

    public CommunityNeoServer server;

    @After
    public void stopServer()
    {
    	if(server != null)
    	{
    		server.stop();
    		server = null;
    	}

        // Clear interrupt flag
        System.out.println("Interrupted: " + Thread.interrupted());
    }

    @Ignore
	@Test
	public void shouldTimeoutIfStartupTakesLongerThanTimeout() throws IOException 
	{
		Configurator configurator = buildProperties();
		configurator.configuration().setProperty(Configurator.STARTUP_TIMEOUT, 1);
		server = createSlowServer(configurator);
		
		try {
			server.start();
			fail("Should have been interrupted.");
		} catch(ServerStartupException e) {
			// ok!
		}
		
	}
    
	@Test
	public void shouldNotFailIfStartupTakesLessTimeThanTimeout() throws IOException 
	{
		Configurator configurator = buildProperties();
		configurator.configuration().setProperty(Configurator.STARTUP_TIMEOUT, 100);
		server = new CommunityNeoServer(configurator){
			@Override
			protected Iterable<ServerModule> createServerModules(){
				return Arrays.asList();
			}
		};

        // When
		try {
			server.start();
		} catch(ServerStartupException e) {
			fail("Should not have been interupted.");
		}

        // Then
        InterruptThreadTimer timer = server.getDependencyResolver().resolveDependency( InterruptThreadTimer.class );

        assertThat(timer.getState(), is( InterruptThreadTimer.State.IDLE));
	}
    
	@Test
	public void shouldNotTimeOutIfTimeoutDisabled() throws IOException 
	{
		Configurator configurator = buildProperties();
		configurator.configuration().setProperty(Configurator.STARTUP_TIMEOUT, 0);
		server = createSlowServer(configurator);

        // When
        server.start();

        // Then
        // No exceptions should have been thrown
	}

	private CommunityNeoServer createSlowServer(Configurator configurator) {
		CommunityNeoServer server = new CommunityNeoServer(configurator){
			@Override
			protected Iterable<ServerModule> createServerModules(){
				ServerModule slowModule = new ServerModule() {
					@Override
					public void start(StringLogger logger) {
						try {
							Thread.sleep(1000 * 5);
						} catch (InterruptedException e) {
                            throw new RuntimeException( e );
						}
					}

					@Override
					public void stop() { }
        		};
				return Arrays.asList(slowModule);
			}
		};
		return server;
	}
	
	private Configurator buildProperties() throws IOException
    {
        new File( test.directory().getAbsolutePath() + DIRSEP + "conf" ).mkdirs();

        Properties databaseProperties = new Properties();
        String databasePropertiesFileName = test.directory().getAbsolutePath() + DIRSEP + "conf"+ DIRSEP +"neo4j.properties";
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );

        Properties serverProperties = new Properties();
        String serverPropertiesFilename = test.directory().getAbsolutePath() + DIRSEP + "conf"+ DIRSEP +"neo4j-server.properties";
        serverProperties.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, test.directory().getAbsolutePath()
                + DIRSEP + "data"+ DIRSEP +"graph.db" );

        serverProperties.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName );
        serverProperties.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, serverPropertiesFilename );
        serverProperties.store( new FileWriter(serverPropertiesFilename), null);
        	
        return new PropertyFileConfigurator(new File(serverPropertiesFilename));
    }
	
}
