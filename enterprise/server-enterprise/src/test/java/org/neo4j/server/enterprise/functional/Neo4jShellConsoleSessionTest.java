/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.enterprise.functional;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;
import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappingDatabase;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.webadmin.console.ConsoleSessionFactory;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.server.webadmin.console.ShellSession;
import org.neo4j.server.webadmin.rest.console.ConsoleService;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore("Due to fix (62e73bfe8265971cebff166bb9ec8a6de3ab8f39) in community not working")
public class Neo4jShellConsoleSessionTest implements ConsoleSessionFactory
{
    private ConsoleService consoleService;
    private final URI uri = URI.create( "http://peteriscool.com:6666/" );
    private Database master;
    private Database slave;
    private ShellSession session;

    @Before
    public void setUp() throws Exception
    {
        TargetDirectory dir = TargetDirectory.forTest( getClass() );
        master = new WrappingDatabase( (AbstractGraphDatabase) new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( dir.directory( "1", true ).getAbsolutePath() )
                .setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE )
                .setConfig( ShellSettings.remote_shell_port, "1337" )
                .setConfig( ClusterSettings.server_id, "1" )
                .setConfig( HaSettings.ha_server, "localhost:6361" )
                .newGraphDatabase() );
        createData( master.getGraph() );
        slave = new WrappingDatabase( (AbstractGraphDatabase) new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( dir.directory( "2", true ).getAbsolutePath() )
                .setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE )
                .setConfig( ShellSettings.remote_shell_port, "1338" )
                .setConfig( ClusterSettings.server_id, "2" )
                .setConfig( HaSettings.ha_server, "localhost:6362" )
                .newGraphDatabase() );
        this.consoleService = new ConsoleService( this, slave, new OutputFormat( new JsonFormat(), uri, null ) );
        this.session = new ShellSession( slave.getGraph() );
    }

    @SuppressWarnings("deprecation")
    private void createData( GraphDatabaseAPI db )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.getReferenceNode().createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            db.getReferenceNode().setProperty( "name", "Test" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @After
    public void shutdownDatabase()
    {
        this.slave.getGraph().shutdown();
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        return session;
    }

    @Override
    public Iterable<String> supportedEngines()
    {
        return null;
    }

    @Test
    public void haMasterSwitchLeavesAWorkingShell() throws Exception
    {
        assertTrue( ((HighlyAvailableGraphDatabase) master.getGraph()).isMaster() );
        issueAndAssertResponse();

        master.getGraph().shutdown();
        try
        {
            slave.getGraph().getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        }
        catch ( Exception e )
        {   // Kind of expected
        }

        assertTrue( ((HighlyAvailableGraphDatabase) slave.getGraph()).isMaster() );
        issueAndAssertResponse();
    }

    private void issueAndAssertResponse() throws Exception
    {
        Response response = consoleService.exec( new JsonFormat(),
                "{ \"command\" : \"ls\", \"engine\":\"shell\" }" );
        assertEquals( 200, response.getStatus() );
        String result = decode( response ).get( 0 );

        assertThat( result, containsString( "Test" ) );
        assertThat( result, containsString( "me" ) );
        assertThat( result, containsString( "-[:TEST]->" ) );
    }

    @SuppressWarnings( "unchecked" )
    private List<String> decode( final Response response ) throws UnsupportedEncodingException, JsonParseException
    {
        return (List<String>) JsonHelper.readJson( new String( (byte[]) response.getEntity(), "UTF-8" ) );
    }
}
