/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import org.junit.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.transactional.error.Neo4jError;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

public class StatementDeserializerTest
{
    @Test
    @SuppressWarnings("unchecked")
    public void shouldDeserializeSingleStatement() throws Exception
    {
        // Given
        String json = createJsonFrom( map( "statements", asList( map( "statement", "Blah blah", "parameters", map( "one", 12 ) ) ) ) );

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
    public void shouldRejectMapWithADifferentFieldBeforeStatement() throws Exception
    {
        // NOTE: We don't really want this behaviour, but it's a symptom of keeping
        // streaming behaviour while moving the statement list into a map.

        String json = "{ \"timeout\" : 200, \"statements\" : [ { \"statement\" : \"ignored\", \"parameters\" : {}} ] }";

        assertYieldsErrors( json,
                new Neo4jError( Status.Request.InvalidFormat,
                        new DeserializationException( "Unable to deserialize request. Expected first field to be 'statements', but was 'timeout'." )));
    }

    @Test
    public void shouldTotallyIgnoreInvalidJsonAfterStatementArrayHasFinished() throws Exception
    {
        // NOTE: We don't really want this behaviour, but it's a symptom of keeping
        // streaming behaviour while moving the statement list into a map.

        // Given
        String json =  "{ \"statements\" : [ { \"statement\" : \"Blah blah\", \"parameters\" : {\"one\" : 12}} ] " +
                "totally invalid json is totally ignored";

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json.getBytes( "UTF-8" ) ) );

        // Then
        assertThat( de.hasNext(), equalTo( true ) );
        Statement stmt = de.next();

        assertThat( stmt.statement(), equalTo( "Blah blah" ) );

        assertThat( de.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldIgnoreUnknownFields() throws Exception
    {
        // Given
        String json =  "{ \"statements\" : [ { \"a\" : \"\", \"b\" : { \"k\":1 }, \"statement\" : \"blah\" } ] }";

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json.getBytes( "UTF-8" ) ) );

        // Then
        assertThat( de.hasNext(), equalTo( true ) );

        assertThat( de.next().statement(), equalTo( "blah" ) );
        assertThat( de.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldTakeParametersBeforeStatement() throws Exception
    {
        // Given
        String json =  "{ \"statements\" : [ { \"a\" : \"\", \"parameters\" : { \"k\":1 }, \"statement\" : \"blah\"}]}";

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json.getBytes( "UTF-8" ) ) );

        // Then
        assertThat( de.hasNext(), equalTo( true ) );

        Statement stmt = de.next();
        assertThat( stmt.statement(), equalTo( "blah" ) );
        assertThat( stmt.parameters(), equalTo( map("k", 1) ) );

        assertThat( de.hasNext(), equalTo( false ) );
    }


    @Test
    public void shouldTreatEmptyInputStreamAsEmptyStatementList() throws Exception
    {
        // Given
        byte[] json = new byte[0];

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json ) );

        // Then
        assertFalse( de.hasNext() );
        assertFalse( de.errors().hasNext() );
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDeserializeMultipleStatements() throws Exception
    {
        // Given
        String json = createJsonFrom( map( "statements", asList(
                map( "statement", "Blah blah", "parameters", map( "one", 12 ) ),
                map( "statement", "Blah bluh", "parameters", map( "asd", asList( "one, two" ) ) ) ) ) );

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
        assertThat( stmt2.parameters(), equalTo( map( "asd", asList( "one, two" ) ) ) );

        assertThat( de.hasNext(), equalTo( false ) );
    }

    @Test
    public void shouldNotThrowButReportErrorOnInvalidInput() throws Exception
    {
        assertYieldsErrors( "{}",
                new Neo4jError( Status.Request.InvalidFormat, new DeserializationException( "Unable to " +
                        "deserialize request. " +
                        "Expected [START_OBJECT, FIELD_NAME, START_ARRAY], " +
                        "found [START_OBJECT, END_OBJECT, null]." ) ) );


        assertYieldsErrors( "{ \"statements\":\"WAIT WAT A STRING NOO11!\" }",
                new Neo4jError( Status.Request.InvalidFormat, new DeserializationException( "Unable to " +
                        "deserialize request. Expected [START_OBJECT, FIELD_NAME, START_ARRAY], found [START_OBJECT, " +
                        "FIELD_NAME, VALUE_STRING]." ) ) );

        assertYieldsErrors( "[{]}",
                new Neo4jError( Status.Request.InvalidFormat,
                        new DeserializationException( "Unable to deserialize request: Unexpected close marker ']': " +
                                "expected '}' " +
                                "(for OBJECT starting at [Source: TestInputStream; line: 1, column: 1])\n " +
                                "at [Source: TestInputStream; line: 1, column: 4]" ) ) );

        assertYieldsErrors( "{ \"statements\" : \"ITS A STRING\" }",
                new Neo4jError( Status.Request.InvalidFormat,
                        new DeserializationException( "Unable to deserialize request. " +
                                "Expected [START_OBJECT, FIELD_NAME, START_ARRAY], " +
                                "found [START_OBJECT, FIELD_NAME, VALUE_STRING]." ) ) );

        assertYieldsErrors( "{ \"statements\" : [ { \"statement\" : [\"dd\"] } ] }",
                new Neo4jError( Status.Request.InvalidFormat,
                        new DeserializationException( "Unable to deserialize request: Can not deserialize instance of" +
                                " java.lang.String out of START_ARRAY token\n at [Source: TestInputStream; line: 1, " +
                                "column: 22]" ) ) );

        assertYieldsErrors( "{ \"statements\" : [ { \"statement\" : \"stmt\", \"parameters\" : [\"AN ARRAY!!\"] } ] }",
                new Neo4jError( Status.Request.InvalidFormat,
                        new DeserializationException( "Unable to deserialize request: Can not deserialize instance of" +
                                " java.util.LinkedHashMap out of START_ARRAY token\n at [Source: TestInputStream; " +
                                "line: 1, column: 42]" ) ) );
    }

    private void assertYieldsErrors( String json, Neo4jError... expectedErrors ) throws UnsupportedEncodingException
    {
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json.getBytes( "UTF-8" ) )
        {
            @Override
            public String toString()
            {
                return "TestInputStream";
            }
        } );
        while ( de.hasNext() )
        {
            de.next();
        }

        Iterator<Neo4jError> actual = de.errors();
        Iterator<Neo4jError> expected = asList( expectedErrors ).iterator();
        while ( actual.hasNext() )
        {
            assertTrue( expected.hasNext() );
            Neo4jError error = actual.next();
            Neo4jError expectedError = expected.next();

            assertThat( error.getMessage(), equalTo( expectedError.getMessage() ) );
            assertThat( error.status(), equalTo( expectedError.status() ) );
        }

        assertFalse( expected.hasNext() );
    }

}
