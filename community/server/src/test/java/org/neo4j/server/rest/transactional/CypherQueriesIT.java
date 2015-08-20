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
package org.neo4j.server.rest.transactional;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;

public class CypherQueriesIT extends AbstractRestFunctionalTestBase
{
    @Test
    public void runningInCompiledRuntime() throws JsonParseException
    {
        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson(
                        "{ 'statements': [ { 'statement': 'CYPHER runtime=compiled MATCH (n) RETURN n' } ] }" ) )
                .post( getDataUri() + "transaction/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
    }

    @Test
    public void includeServerExecutionTime() throws Exception
    {
        // when
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson(
                        "{ 'statements': [ { 'statement': 'MATCH (n) RETURN n' } ] }" ) )
                .post( getDataUri() + "transaction/commit" );

        // then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
        Object serverTime = result.get( "serverTimeNanos" );
        assertThat( serverTime, anyOf( instanceOf( Integer.class ), instanceOf( Long.class ) ) );
        long nanos = ((Number) serverTime).longValue();
        assertTrue( "serverTimeNanos should be positive", nanos > 0 );
    }


    private void assertNoErrors( Map<String, Object> response )
    {
        @SuppressWarnings("unchecked")
        Iterator<Map<String, Object>> errors = ((List<Map<String, Object>>) response.get( "errors" )).iterator();
        assertFalse( errors.hasNext() );
    }

    private String quotedJson( String singleQuoted )
    {
        return singleQuoted.replaceAll( "'", "\"" );
    }

}
