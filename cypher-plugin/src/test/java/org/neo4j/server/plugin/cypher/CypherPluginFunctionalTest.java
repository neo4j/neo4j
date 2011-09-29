/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.plugin.cypher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import javax.ws.rs.core.Response.Status;

import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class CypherPluginFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static final String ENDPOINT = "http://localhost:7474/db/data/ext/CypherPlugin/graphdb/execute_query";

    /**
     * A simple query returning all nodes connected to node 1, returning the
     * node and the name property, if it exists, otherwise `null`:
     */
    @Test
    @Documented
    @Title( "Send a Query" )
    @Graph( nodes = {
            @NODE( name = "I", setNameProperty = true ),
            @NODE( name = "you", setNameProperty = true ),
            @NODE( name = "him", setNameProperty = true, properties = { 
                    @PROP( key = "age", value = "25", type = GraphDescription.PropType.INTEGER ) } ) }, 
            relationships = {
            @REL( start = "I", end = "him", type = "know", properties = {} ),
            @REL( start = "I", end = "you", type = "know", properties = {} ) } )
    public void testPropertyColumn() throws UnsupportedEncodingException
    {
        String script = "start x  = (" + data.get().get( "I" ).getId()
                        + ") match (x) -[r]-> (n) return type(r), n.name?, n.age?";
        gen.get().expectedStatus( Status.OK.getStatusCode() ).payload(
                "{\"query\": \"" + script + "\"}" ).description(
                        AsciidocHelper.createCypherSnippet(  script ) );
        String response = gen.get().post( ENDPOINT ).entity();
        assertTrue( response.contains( "you" ) );
        assertTrue( response.contains( "him" ) );
        assertTrue( response.contains( "25" ) );
        assertTrue( !response.contains( "\"x\"" ) );
    }
    
    /**
     * Errors on the server will be reported as a JSON-formatted stacktrace and
     * message.
     */
    @Test
    @Documented
    @Title( "Return on syntax errors" )
    @Graph( "I know you" )
    public void error_gets_returned_as_json() throws UnsupportedEncodingException, Exception
    {
        String script = "start x  = (" + data.get().get( "I" ).getId()
                        + ") return x.dummy";
        gen.get().expectedStatus( Status.BAD_REQUEST.getStatusCode() ).payload(
                "{\"query\": \"" + script + "\"}" ).description(
                AsciidocHelper.createCypherSnippet(  script ) );
        String response = gen.get().post( ENDPOINT ).entity();
        assertEquals( 3, ( JsonHelper.jsonToMap( response ) ).size() );
    }

    

    
    /**
     * Paths can be returned
     * together with other return types by just
     * specifying returns.
     */
    @Test
    @Documented
    @Graph( "I know you" )
    public void return_paths() throws UnsupportedEncodingException, Exception
    {
        String script = "start x  = (" + data.get().get( "I" ).getId()
                        + ") match path = (x--friend) return path, friend.name";
        gen.get().expectedStatus( Status.OK.getStatusCode() ).payload(
                "{\"query\": \"" + script + "\"}" ).description(
                        AsciidocHelper.createCypherSnippet( script ));
        String response = gen.get().post( ENDPOINT ).entity();
        assertEquals( 2, ( JsonHelper.jsonToMap( response ) ).size() );
        assertTrue(response.contains( "data" ));
        assertTrue(response.contains( "you" ));
    }
    
    /**
     * The plugin can return a JSONTable representation
     * of the results. For details, see 
     * http://code.google.com/apis/chart/interactive/docs/reference.html#dataparam[Google Data Table Format]
     */
    @Test
    @Documented
    @Graph( "I know you" )
    public void return_JSON_table_format() throws UnsupportedEncodingException, Exception
    {
        String script = "start x  = (" + data.get().get( "I" ).getId()
                        + ") match path = (x--friend) return path, friend.name";
        gen.get().expectedStatus( Status.OK.getStatusCode() ).payload(
                "{\"query\": \"" + script + "\",\"format\": \"json-data-table\"}" ).description(
                        AsciidocHelper.createCypherSnippet( script ));
        String response = gen.get().post( ENDPOINT ).entity();
        assertEquals( 2, ( JsonHelper.jsonToMap( response ) ).size() );
        assertTrue(response.contains( "cols" ));
        assertTrue(response.contains( "rows" ));
    }
}
