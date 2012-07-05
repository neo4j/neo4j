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
package org.neo4j.server.webadmin.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappingDatabase;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

public class Neo4jShellConsoleSessionTest implements SessionFactory
{
    private static final String LN = System.getProperty("line.separator");
    private ConsoleService consoleService;
    private Database database;
    private final URI uri = URI.create( "http://peteriscool.com:6666/" );

    @Before
    public void setUp() throws Exception
    {
        this.database = new WrappingDatabase( (AbstractGraphDatabase) new TestGraphDatabaseFactory().
            newImpermanentDatabaseBuilder().
            setConfig( ShellSettings.remote_shell_enabled, GraphDatabaseSetting.TRUE ).
            newGraphDatabase() );
        this.consoleService = new ConsoleService( this, database, new OutputFormat( new JsonFormat(), uri, null ) );
    }

    @After
    public void shutdownDatabase()
    {
        this.database.getGraph().shutdown();
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        return new ShellSession( database.getGraph() );
    }

    @Test
    public void doesntMangleNewlines() throws Exception
    {
        Response response = consoleService.exec( new JsonFormat(),
                "{ \"command\" : \"start n=node(0) return n;\", \"engine\":\"shell\" }" );

     
        assertEquals( 200, response.getStatus() );
        String result = decode( response ).get(0);

        String expected = "+-----------+"+LN
                         +"| n         |"+LN
                         +"+-----------+"+LN
                         +"| Node[0]{} |"+LN
                         +"+-----------+"+LN
                         +"1 row";
        
        assertThat( result, containsString( expected ) );
    }
    
    private List<String> decode( final Response response ) throws UnsupportedEncodingException, JsonParseException
    {
        return (List<String>)JsonHelper.readJson(new String( (byte[]) response.getEntity(), "UTF-8" ));
    }
}
