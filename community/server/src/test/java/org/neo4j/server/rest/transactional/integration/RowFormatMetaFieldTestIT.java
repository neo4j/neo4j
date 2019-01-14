/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.rest.transactional.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.matches;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsAMetaListAtIndex;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsMetaNodesAtIndex;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsMetaRelsAtIndex;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class RowFormatMetaFieldTestIT extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( server().baseUri() );

    private String commitResource;

    @Before
    public void setUp()
    {
        // begin
        Response begin = http.POST( "db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );
        try
        {
            commitResource = begin.stringFromContent( "commit" );
        }
        catch ( JsonParseException e )
        {
            fail( "Exception caught when setting up test: " + e.getMessage() );
        }
        assertThat( commitResource, equalTo( begin.location() + "/commit" ) );
    }

    @After
    public void tearDown()
    {
        // empty the database
        graphdb().execute( "MATCH (n) DETACH DELETE n" );
    }

    @Test
    public void metaFieldShouldGetCorrectIndex()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s:Start)-[r:R]->(e:End) RETURN s, r, 1, e" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsMetaNodesAtIndex( 0, 3 ) );
        assertThat( commit, rowContainsMetaRelsAtIndex( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
    }

    @Test
    public void metaFieldShouldGivePathInfoInList()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH p=(s)-[r:R]->(e) RETURN p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsAMetaListAtIndex( 0 ) );
        assertThat( commit.status(), equalTo( 200 ) );
    }

    @Test
    public void metaFieldShouldPutPathListAtCorrectIndex()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH p=(s)-[r:R]->(e) RETURN 10, p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsAMetaListAtIndex( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
    }

    private HTTP.RawPayload queryAsJsonRow( String query )
    {
        return quotedJson( "{ 'statements': [ { 'statement': '" + query + "', 'resultDataContents': [ 'row' ] } ] }" );
    }

    private void assertHasTxLocation( Response begin )
    {
        assertThat( begin.location(), matches( "http://localhost:\\d+/db/data/transaction/\\d+" ) );
    }
}
