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
package org.neo4j.server.rest.transactional;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.neo4j.server.rest.transactional.error.InvalidRequestError;
import org.neo4j.server.rest.transactional.error.Neo4jError;

public class StatementDeserializerTest
{

    @Test
    public void shouldDeserializeSingleStatement() throws Exception
    {
        // Given
        String json = createJsonFrom( asList( map( "statement", "Blah blah", "parameters", map( "one", 12 ) ) ) );

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json.getBytes( "UTF-8" ) ) );

        // Then
        assertThat( de.hasNext(), equalTo( true ) );
        Statement stmt = de.next();

        assertThat( stmt.statement(), equalTo( "Blah blah" ) );
        assertThat( stmt.parameters(), equalTo( map( "one", 12 ) ) );

        assertThat( de.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldDeserializeMultipleStatements() throws Exception
    {
        // Given
        String json = createJsonFrom( asList(
                map( "statement", "Blah blah", "parameters", map( "one", 12 )),
                map( "statement", "Blah bluh", "parameters", map( "asd", asList("one, two") ))));

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json.getBytes( "UTF-8" ) ) );

        // Then
        assertThat( de.hasNext(), equalTo( true ) );
        Statement stmt = de.next();

        assertThat( stmt.statement(), equalTo( "Blah blah" ) );
        assertThat( stmt.parameters(), equalTo( map( "one", 12 ) ) );

        assertThat( de.hasNext(), equalTo( true ) );
        Statement stmt2 = de.next();

        assertThat( stmt2.statement(), equalTo( "Blah bluh" ) );
        assertThat( stmt2.parameters(), equalTo( map( "asd", asList("one, two") ) ) );

        assertThat( de.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldNotThrowButReportErrorOnInvalidInput() throws Exception
    {
        assertYieldsErrors( "{}", Arrays.<Neo4jError>asList(
                new InvalidRequestError( "Unable to deserialize request, expected START_ARRAY, found START_OBJECT." )));
        assertYieldsErrors( "[{]}", Arrays.<Neo4jError>asList(
                new InvalidRequestError(
                        "Unable to deserialize request: Unexpected close marker ']': expected '}' " +
                        "(for OBJECT starting at [Source: TestInputStream; line: 1, column: 1])\n " +
                        "at [Source: TestInputStream; line: 1, column: 4]" )));
    }

    private void assertYieldsErrors( String json, List<Neo4jError> expectedList ) throws UnsupportedEncodingException
    {
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json.getBytes( "UTF-8" ) ) {
            @Override
            public String toString()
            {
                return "TestInputStream";
            }
        });
        while(de.hasNext()) de.next();

        Iterator<Neo4jError> actual = de.errors();
        Iterator<Neo4jError> expected = expectedList.iterator();
        while(actual.hasNext())
        {
            assertTrue( expected.hasNext() );
            Neo4jError error = actual.next();
            Neo4jError expectedError = expected.next();

            assertThat( error.getErrorCode(), equalTo( expectedError.getErrorCode() ) );
            assertThat( error.getMessage(), equalTo( expectedError.getMessage() ) );
        }

        assertFalse( expected.hasNext() );
    }

}
