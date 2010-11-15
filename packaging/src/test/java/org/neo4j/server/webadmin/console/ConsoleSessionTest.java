package org.neo4j.server.webadmin.console;

import org.junit.Test;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.database.Database;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConsoleSessionTest
{
    @Test
    public void retrievesTheReferenceNode()
    {
        ConsoleSession session = new ConsoleSession( new Database( new ImpermanentGraphDatabase( "target/tempdb" ) ) );
        List<String> result = session.evaluate( "$_" );

        assertThat( result.get( 0 ), is( "v[0]" ) );
    }

    @Test
    public void canCreateNodesInGremlinLand()
    {
        ConsoleSession session = new ConsoleSession( new Database( new ImpermanentGraphDatabase( "target/tempdb" ) ) );
        List<String> result = session.evaluate( "g:add-v()" );

        assertThat( result.get( 0 ), is( "v[1]" ) );
    }
}

