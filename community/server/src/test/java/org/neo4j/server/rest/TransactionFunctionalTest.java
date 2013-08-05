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
package org.neo4j.server.rest;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.rest.repr.util.RFC1123;
import org.neo4j.server.rest.transactional.error.StatusCode;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.server.configuration.Configurator.DEFAULT_TRANSACTION_TIMEOUT;
import static org.neo4j.server.configuration.Configurator.TRANSACTION_TIMEOUT;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class TransactionFunctionalTest extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( "http://localhost:7474" );

    @Test
    public void begin__execute__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        long startOfTest = System.currentTimeMillis();

        // This generous wait time is necessary to compensate for limited resolution of RFC 1123 timestamps
        // and the fact that the system clock is allowed to run "backwards" between threads
        // (cf. http://stackoverflow.com/questions/2978598)
        //
        Thread.sleep( 3000 );

        // begin
        Response begin = http.POST( "/db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertThat( begin.location(), matches( "http://localhost:\\d+/db/data/transaction/\\d+" ) );

        long beginStamp = expirationTime( jsonToMap( begin.rawContent() ) );
        long defaultTimeoutMillis =
            SECONDS.toMillis( server().getConfiguration().getInt( TRANSACTION_TIMEOUT, DEFAULT_TRANSACTION_TIMEOUT ) );
        assertTrue( beginStamp > (defaultTimeoutMillis + startOfTest ) );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource, matches( "http://localhost:\\d+/db/data/transaction/\\d+/commit" ) );

        Thread.sleep( 3000 );

        // execute
        Response execute =
            http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );

        assertThat( execute.status(), equalTo( 200 ) );
        long executeStamp = expirationTime( jsonToMap( execute.rawContent() ) );
        assertTrue( executeStamp > startOfTest );
        assertTrue( executeStamp > beginStamp );

        // commit
        Response commit = http.POST( commitResource );

        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin__execute__rollback() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "/db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertThat( begin.location(), matches( "http://localhost:\\d+/db/data/transaction/\\d+" ) );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );

        // rollback
        Response commit = http.DELETE( begin.location() );

        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }

    @Test
    public void begin__execute_and_commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "/db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertThat( begin.location(), containsString( "/db/data/transaction" ) );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource, equalTo( begin.location() + "/commit" ) );

        // execute and commit
        Response commit = http.POST( commitResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );
        assertNoErrors( (Map<String, Object>) commit.content() );

        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin_and_execute__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute
        Response begin = http.POST( "/db/data/transaction", quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' " +
                "} ] }" ) );

        String commitResource = begin.stringFromContent( "commit" );

        // commit
        Response commit = http.POST( commitResource );

        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin__execute__commit__execute() throws Exception
    {
        // begin
        Response begin = http.POST( "/db/data/transaction" );
        String commitResource = begin.stringFromContent( "commit" );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );

        // commit
        http.POST( commitResource );

        // execute
        Response execute =
            http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );

        assertThat( execute.status(), equalTo( 404 ) );
        assertErrorCodes( execute, StatusCode.INVALID_TRANSACTION_ID );
    }

    @Test
    public void begin_and_execute_and_commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute and commit
        Response begin = http.POST( "/db/data/transaction/commit", quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );

        assertThat( begin.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin__execute_multiple__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "/db/data/transaction" );

        String commitResource = begin.stringFromContent( "commit" );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' }, { 'statement': 'CREATE n' } ] }" ) );

        // commit
        assertNoErrors( http.POST( commitResource ) );

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 2 ) );
    }

    @Test
    public void begin__execute__execute__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "/db/data/transaction" );

        String commitResource = begin.stringFromContent( "commit" );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );

        // commit
        http.POST( commitResource );

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 2 ) );
    }

    @Test
    public void begin__commit_with_invalid_cypher() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response response = POST( getDataUri() + "transaction", quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );
        String commitResource = response.stringFromContent( "commit" );

        // commit with invalid cypher
        response = POST( commitResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE ;;' } ] }" ) );

        // System.out.println( "begin.rawContent() = " + response.rawContent() );

        assertThat( response.status(), is( 200 ) );
        assertErrorCodes( response, StatusCode.STATEMENT_SYNTAX_ERROR );
        assertNoStackTrace( response );

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }

    @Test
    public void begin__commit_with_malformed_json() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST( getDataUri() + "transaction", quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );
        String commitResource = begin.stringFromContent( "commit" );

        // commit with malformed json
        Response response = POST( commitResource, rawPayload( "[{asd,::}]" ) );

        assertThat( response.status(), is( 200 ) );
        assertErrorCodes( response, StatusCode.INVALID_REQUEST_FORMAT );

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }
    
    @Test
    public void begin_create_two_nodes_delete_one() throws Exception
    {
        /*
         * This issue was reported from the community. It resulted in a refactoring of the interaction
         * between TxManager and TransactionContexts.
         */
        
        // GIVEN
        long nodesInDatabaseBeforeTransaction = countNodes();
        Response response = http.POST( "/db/data/transaction/commit",
                rawPayload( "{ \"statements\" : [{\"statement\" : \"CREATE (n0:DecibelEntity :AlbumGroup{DecibelID : '34a2201b-f4a9-420f-87ae-00a9c691cc5c', Title : 'Dance With Me', ArtistString : 'Ra Ra Riot', MainArtistAlias : 'Ra Ra Riot', OriginalReleaseDate : '2013-01-08', IsCanon : 'False'}) return id(n0)\"}, {\"statement\" : \"CREATE (n1:DecibelEntity :AlbumRelease{DecibelID : '9ed529fa-7c19-11e2-be78-bcaec5bea3c3', Title : 'Dance With Me', ArtistString : 'Ra Ra Riot', MainArtistAlias : 'Ra Ra Riot', LabelName : 'Barsuk Records', FormatNames : 'File', TrackCount : '3', MediaCount : '1', Duration : '460.000000', ReleaseDate : '2013-01-08', ReleaseYear : '2013', ReleaseRegion : 'USA', Cline : 'Barsuk Records', Pline : 'Barsuk Records', CYear : '2013', PYear : '2013', ParentalAdvisory : 'False', IsLimitedEdition : 'False'}) return id(n1)\"}]}" ) );
        assertEquals( 200, response.status() );
        JsonNode everything = jsonNode( response.rawContent() );
        JsonNode result = everything.get( "results" ).get( 0 );
        long id = result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).getLongValue();
        
        // WHEN
        http.POST( "/db/data/cypher", rawPayload( "{\"query\":\"start n = node(" + id + ") delete n\"}" ) );

        // THEN
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction+1 ) );
    }

    @Test
    public void should_include_graph_format_when_requested() throws Exception
    {
        // given
        http.POST( "/db/data/transaction/commit", singleStatement( "CREATE (n:Foo:Bar)" ) );

        // when
        Response response = http.POST( "/db/data/transaction/commit", quotedJson(
                "{ 'statements': [ { 'statement': 'MATCH (n:Foo) RETURN n', 'resultDataContents':['row','graph'] } ] }" ) );

        // then
        assertThat( response.status(), equalTo( 200 ) );
        JsonNode data = response.jsonContent().get( "results" ).get( 0 ).get( "data" );
        assertTrue( "data is a list", data.isArray() );
        assertEquals( "one entry", 1, data.size() );
        JsonNode entry = data.get( 0 );
        assertTrue( "entry has row", entry.has( "row" ) );
        assertTrue( "entry has graph", entry.has( "graph" ) );
        JsonNode nodes = entry.get( "graph" ).get( "nodes" ), rels = entry.get( "graph" ).get( "relationships" );
        assertTrue( "nodes is a list", nodes.isArray() );
        assertTrue( "relationships is a list", rels.isArray() );
        assertEquals( "one node", 1, nodes.size() );
        assertEquals( "no relationships", 0, rels.size() );
        Set<String> labels = new HashSet<>();
        for ( JsonNode node : nodes.get( 0 ).get( "labels" ) )
        {
            labels.add( node.getTextValue() );
        }
        assertEquals( "labels", asSet( "Foo", "Bar" ), labels );
    }

    private HTTP.RawPayload singleStatement( String statement )
    {
        return rawPayload( "{\"statements\":[{\"statement\":\"" + statement + "\"}]}" );
    }

    private void assertNoErrors( Response response )
    {
        assertErrorCodes( response.<Map<String, Object>>content() );
    }

    private void assertNoErrors( Map<String, Object> response )
    {
        assertErrorCodes( response );
    }

    private void assertNoStackTrace( Response response )
    {
        Map<String, Object> content = response.content();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> errors = ((List<Map<String, Object>>) content.get( "errors" ));

        for ( Map<String, Object> error : errors )
        {
            assertFalse( error.containsKey( "stackTrace" ) );
        }
    }

    private void assertErrorCodes( Response response, StatusCode... expectedErrors )
    {
        assertErrorCodes( response.<Map<String, Object>>content(), expectedErrors );
    }

    private void assertErrorMessages( Response response, String... expectedMessages )
    {
        assertErrorMessages( response.<Map<String, Object>>content(), expectedMessages );
    }

    @SuppressWarnings("unchecked")
    private void assertErrorCodes( Map<String, Object> response, StatusCode... expectedErrors )
    {
        Iterator<Map<String, Object>> errors = ((List<Map<String, Object>>) response.get( "errors" )).iterator();
        Iterator<StatusCode> expected = iterator( expectedErrors );

        while ( expected.hasNext() )
        {
            assertTrue( errors.hasNext() );
            assertThat( (Integer) errors.next().get( "code" ), equalTo( expected.next().getCode() ) );
        }
        if ( errors.hasNext() )
        {
            Map<String, Object> error = errors.next();
            fail( "Expected no more errors, but got " + error.get( "code" ) + " - '" + error.get( "message" ) + "'." );
        }
    }

    @SuppressWarnings("unchecked")
    private void assertErrorMessages( Map<String, Object> response, String... expectedMessages )
    {
        Iterator<Map<String, Object>> errors = ((List<Map<String, Object>>) response.get( "errors" )).iterator();
        Iterator<String> expected = iterator( expectedMessages );

        while ( expected.hasNext() )
        {
            assertTrue( errors.hasNext() );
            assertThat( (String) errors.next().get( "message" ), equalTo( expected.next() ) );
        }
        if ( errors.hasNext() )
        {
            Map<String, Object> error = errors.next();
            fail( "Expected no more errors, but got " + error.get( "message" ) + " - '" + error.get( "message" ) + "'." );
        }
    }

    @SuppressWarnings("WhileLoopReplaceableByForEach")
    private long countNodes()
    {
        Transaction transaction = graphdb().beginTx();
        try
        {
            long count = 0;
            Iterator<Node> allNodes = GlobalGraphOperations.at( graphdb() ).getAllNodes().iterator();
            while ( allNodes.hasNext() )
            {
                allNodes.next();
                count++;
            }
            return count;
        }
        finally
        {
            transaction.finish();
        }
    }

    private static Matcher<String> matches( final String pattern )
    {
        final Pattern regex = Pattern.compile( pattern );

        return new TypeSafeMatcher<String>()
        {
            @Override
            protected boolean matchesSafely( String item )
            {
                return regex.matcher( item ).matches();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "matching regex" ).appendValue( pattern );
            }
        };
    }

    private long expirationTime( Map<String, Object> entity ) throws ParseException
    {
        String timestampString = (String) ( (Map<?, ?>) entity.get( "transaction" ) ).get( "expires" );
        return RFC1123.parseTimestamp( timestampString ).getTime();
    }
}
