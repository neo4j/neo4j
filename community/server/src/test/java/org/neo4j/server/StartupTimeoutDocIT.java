/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.helpers.Transactor;
import org.neo4j.server.helpers.UnitOfWork;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Platforms.platformIsWindows;

public class StartupTimeoutDocIT
{
    TargetDirectory target = TargetDirectory.forTest( StartupTimeoutDocIT.class );
    private static final String DIRSEP = File.separator;

    @Rule
    public TargetDirectory.TestDirectory test = target.testDirectory();

    public CommunityNeoServer server;

    @After
    public void stopServer()
    {
        if ( server != null )
        {
            server.stop();
            server = null;
        }
    }

    @Test
    public void shouldTimeoutIfStartupTakesLongerThanTimeout() throws IOException
    {
        if(platformIsWindows())
            return;

        Configurator configurator = buildProperties();
        configurator.configuration().setProperty( Configurator.STARTUP_TIMEOUT, 1 );
        server = createSlowServer( configurator );

        try
        {
            server.start();
            fail( "Should have been interrupted." );
        }
        catch ( ServerStartupException e )
        {
            // ok!
        }
    }

	@Test
	public void shouldNotFailIfStartupTakesLessTimeThanTimeout() throws IOException 
	{
		Configurator configurator = buildProperties();
		configurator.configuration().setProperty(Configurator.STARTUP_TIMEOUT, 100);
        server = new CommunityNeoServer( configurator )
        {
            @Override
            protected Iterable<ServerModule> createServerModules()
            {
                return Arrays.asList();
            }
        };

        // When
        try
        {
            server.start();
        }
        catch ( ServerStartupException e )
        {
            fail( "Should not have been interupted." );
        }

        // Then
        InterruptThreadTimer timer = server.getDependencyResolver().resolveDependency( InterruptThreadTimer.class );

        assertThat(timer.getState(), is( InterruptThreadTimer.State.IDLE));
	}
    
	@Test
	public void shouldNotTimeOutIfTimeoutDisabled() throws IOException 
	{
        Configurator configurator = buildProperties();
        configurator.configuration().setProperty( Configurator.STARTUP_TIMEOUT, 0 );
        server = createSlowServer( configurator );

        // When
        server.start();

        // Then
        // No exceptions should have been thrown
	}

    private CommunityNeoServer createSlowServer( Configurator configurator )
    {
        CommunityNeoServer server = new CommunityNeoServer( configurator )
        {
            @Override
            protected Iterable<ServerModule> createServerModules()
            {
                ServerModule slowModule = new ServerModule()
                {
                    @Override
                    public void start( StringLogger logger )
                    {
                        try
                        {
                            Thread.sleep( 1000 * 5 );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }

                    @Override
                    public void stop()
                    {
                    }
                };
                return Arrays.asList( slowModule );
            }
        };
        return server;
	}
	
    private Configurator buildProperties() throws IOException
    {
        new File( test.directory().getAbsolutePath() + DIRSEP + "conf" ).mkdirs();

        Properties databaseProperties = new Properties();
        String databasePropertiesFileName = test.directory().getAbsolutePath() + DIRSEP + "conf" + DIRSEP
                + "neo4j.properties";
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );

        Properties serverProperties = new Properties();
        String serverPropertiesFilename = test.directory().getAbsolutePath() + DIRSEP + "conf" + DIRSEP
                + "neo4j-server.properties";
        serverProperties.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, test.directory().getAbsolutePath()
                + DIRSEP + "data" + DIRSEP + "graph.db" );

        serverProperties.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName );
        serverProperties.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, serverPropertiesFilename );
        serverProperties.store( new FileWriter( serverPropertiesFilename ), null );

        return new PropertyFileConfigurator( new File( serverPropertiesFilename ) );
    }



    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Test
    public void shoulWOrk() throws Exception
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();

        clearAll();
        
        tx = db.beginTx();
        db.createNode();
        tx.success();tx.finish();

        clearAll();
    }

    private void clearAll()
    {
        new Transactor( dbRule.getGraphDatabaseService(), new UnitOfWork()
        {
            @Override
            public void doWork()
            {
                deleteAllNodesAndRelationships( dbRule.getGraphDatabaseService() );

                deleteAllIndexes( dbRule.getGraphDatabaseService() );
            }

            private void deleteAllNodesAndRelationships( final GraphDatabaseService db )
            {
                Iterable<Node> allNodes = GlobalGraphOperations.at(db).getAllNodes();
                for ( Node n : allNodes )
                {
                    Iterable<Relationship> relationships = n.getRelationships();
                    for ( Relationship rel : relationships )
                    {
                        rel.delete();
                    }
                    if ( n.getId() != 0 )
                    { // Don't delete the reference node - tests depend on it
                        // :-(
                        n.delete();
                    }
                    else
                    { // Remove all state from the reference node instead
                        for ( String key : n.getPropertyKeys() )
                        {
                            n.removeProperty( key );
                        }
                    }
                }
            }

            private void deleteAllIndexes( final GraphDatabaseService db )
            {
                IndexManager indexManager = db.index();

                for ( String indexName : indexManager.nodeIndexNames() )
                {
                    try
                    {
                        db.index().forNodes( indexName ).delete();
                    }
                    catch ( UnsupportedOperationException e )
                    {
                        // Encountered a read-only index.
                    }
                }

                for ( String indexName : indexManager.relationshipIndexNames() )
                {
                    try
                    {
                        db.index().forRelationships( indexName ).delete();
                    }
                    catch ( UnsupportedOperationException e )
                    {
                        // Encountered a read-only index.
                    }
                }

                for ( String k : indexManager.getNodeAutoIndexer().getAutoIndexedProperties() )
                {
                    indexManager.getNodeAutoIndexer().stopAutoIndexingProperty( k );
                }
                indexManager.getNodeAutoIndexer().setEnabled( false );

                for ( String k : indexManager.getRelationshipAutoIndexer().getAutoIndexedProperties() )
                {
                    indexManager.getRelationshipAutoIndexer().stopAutoIndexingProperty( k );
                }
                indexManager.getRelationshipAutoIndexer().setEnabled( false );
            }
        } ).execute();
    }
}
