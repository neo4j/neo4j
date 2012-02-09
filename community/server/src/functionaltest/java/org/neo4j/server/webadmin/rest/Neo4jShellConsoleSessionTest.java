package org.neo4j.server.webadmin.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.Config;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.test.ImpermanentGraphDatabase;

public class Neo4jShellConsoleSessionTest implements SessionFactory
{
    private ConsoleService consoleService;
    private Database database;
    private final URI uri = URI.create( "http://peteriscool.com:6666/" );

    @Before
    public void setUp() throws Exception
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put(Config.ENABLE_REMOTE_SHELL, "true");
        this.database = new Database( new ImpermanentGraphDatabase(params) );
        this.consoleService = new ConsoleService( this, database, new OutputFormat( new JsonFormat(), uri, null ) );
    }

    @After
    public void shutdownDatabase()
    {
        this.database.shutdown();
    }

    @Override
    public ScriptSession createSession( String engineName, Database database )
    {
        return new ShellSession( database.graph );
    }

    @Test
    public void doesntMangleNewlines() throws UnsupportedEncodingException
    {
        Response response = consoleService.exec( new JsonFormat(),
                "{ \"command\" : \"start n=node(0) return n\", \"engine\":\"shell\" }" );

     
        assertEquals( 200, response.getStatus() );
        String result = decode( response );

        // Awful hack: Result contains a timestamp for how
        // long the query took, remove that timestamp to get 
        // a deterministic test.
        result = result.replaceAll("\\d+ ms", "");
        
        String expected = "[ \"+-----------+\\n| n         |\\n+-----------+\\n| Node[0]{} |\\n+-----------+\\n1 rows, \\n\\n\", \"neo4j-sh (0)$ \" ]";

        assertThat( result, is(expected) );
    }
    
    private String decode( final Response response ) throws UnsupportedEncodingException
    {
        return new String( (byte[]) response.getEntity(), "UTF-8" );
    }
}
