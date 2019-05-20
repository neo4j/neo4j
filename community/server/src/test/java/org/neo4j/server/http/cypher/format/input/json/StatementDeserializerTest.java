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
package org.neo4j.server.http.cypher.format.input.json;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.server.http.cypher.format.api.InputFormatException;
import org.neo4j.string.UTF8;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

public class StatementDeserializerTest
{
    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldDeserializeSingleStatement()
    {
        // Given
        String json = createJsonFrom( map( "statements", asList( map( "statement", "Blah blah", "parameters", map( "one", 12 ) ) ) ) );

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( UTF8.encode( json ) ) );

        // Then
        InputStatement stmt = de.read();
        assertNotNull( stmt );

        assertThat( stmt.statement(), equalTo( "Blah blah" ) );
        assertThat( stmt.parameters(), equalTo( map( "one", 12 ) ) );

        assertNull( de.read() );
    }

    @Test
    public void shouldRejectMapWithADifferentFieldBeforeStatement()
    {
        // NOTE: We don't really want this behaviour, but it's a symptom of keeping
        // streaming behaviour while moving the statement list into a map.

        String json = "{ \"timeout\" : 200, \"statements\" : [ { \"statement\" : \"ignored\", \"parameters\" : {}} ] }";

        assertYieldsErrors( json, "Unable to deserialize request. Expected first field to be 'statements', but was 'timeout'." );
    }

    @Test
    public void shouldTotallyIgnoreInvalidJsonAfterStatementArrayHasFinished()
    {
        // NOTE: We don't really want this behaviour, but it's a symptom of keeping
        // streaming behaviour while moving the statement list into a map.

        // Given
        String json =  "{ \"statements\" : [ { \"statement\" : \"Blah blah\", \"parameters\" : {\"one\" : 12}} ] " +
                "totally invalid json is totally ignored";

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( UTF8.encode( json ) ) );

        // Then
        InputStatement stmt = de.read();
        assertNotNull( stmt );

        assertThat( stmt.statement(), equalTo( "Blah blah" ) );

        assertNull( de.read() );
    }

    @Test
    public void shouldIgnoreUnknownFields()
    {
        // Given
        String json =  "{ \"statements\" : [ { \"a\" : \"\", \"b\" : { \"k\":1 }, \"statement\" : \"blah\" } ] }";

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( UTF8.encode( json ) ) );

        // Then
        InputStatement stmt = de.read();
        assertNotNull( stmt );

        assertThat( stmt.statement(), equalTo( "blah" ) );

        assertNull( de.read() );
    }

    @Test
    public void shouldTakeParametersBeforeStatement()
    {
        // Given
        String json =  "{ \"statements\" : [ { \"a\" : \"\", \"parameters\" : { \"k\":1 }, \"statement\" : \"blah\"}]}";

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( UTF8.encode( json ) ) );

        // Then
        InputStatement stmt = de.read();
        assertNotNull( stmt );
        assertThat( stmt.statement(), equalTo( "blah" ) );
        assertThat( stmt.parameters(), equalTo( map("k", 1) ) );

        assertNull( de.read() );
    }

    @Test
    public void shouldTreatEmptyInputStreamAsEmptyStatementList()
    {
        // Given
        byte[] json = new byte[0];

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( json ) );

        // Then
        assertNull( de.read() );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldDeserializeMultipleStatements()
    {
        // Given
        String json = createJsonFrom( map( "statements", asList(
                map( "statement", "Blah blah", "parameters", map( "one", 12 ) ),
                map( "statement", "Blah bluh", "parameters", map( "asd", asList( "one, two" ) ) ) ) ) );

        // When
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( UTF8.encode( json ) ) );

        // Then
        InputStatement stmt = de.read();
        assertNotNull( stmt );

        assertThat( stmt.statement(), equalTo( "Blah blah" ) );
        assertThat( stmt.parameters(), equalTo( map( "one", 12 ) ) );

        InputStatement stmt2 = de.read();
        assertNotNull( stmt2 );

        assertThat( stmt2.statement(), equalTo( "Blah bluh" ) );
        assertThat( stmt2.parameters(), equalTo( map( "asd", asList( "one, two" ) ) ) );

        assertNull( de.read() );
    }

    @Test
    public void shouldNotThrowButReportErrorOnInvalidInput()
    {
        assertYieldsErrors( "{}", "Unable to " +
                        "deserialize request. " +
                        "Expected [START_OBJECT, FIELD_NAME, START_ARRAY], " +
                        "found [START_OBJECT, END_OBJECT, null]." );

        assertYieldsErrors( "{ \"statements\":\"WAIT WAT A STRING NOO11!\" }",
                "Unable to " +
                        "deserialize request. Expected [START_OBJECT, FIELD_NAME, START_ARRAY], found [START_OBJECT, " +
                        "FIELD_NAME, VALUE_STRING]."  );

        assertYieldsErrors( "[{]}",
                "Could not parse the incoming JSON", "Unexpected close marker ']': " +
                                "expected '}' " +
                                "(for OBJECT starting at [Source: TestInputStream; line: 1, column: 1])\n " +
                                "at [Source: TestInputStream; line: 1, column: 4]" );

        assertYieldsErrors( "{ \"statements\" : \"ITS A STRING\" }",
                         "Unable to deserialize request. " +
                                "Expected [START_OBJECT, FIELD_NAME, START_ARRAY], " +
                                "found [START_OBJECT, FIELD_NAME, VALUE_STRING]." );

        assertYieldsErrors( "{ \"statements\" : [ { \"statement\" : [\"dd\"] } ] }",
                "Could not map the incoming JSON", "Can not deserialize instance of" +
                                " java.lang.String out of START_ARRAY token\n at [Source: TestInputStream; line: 1, " +
                                "column: 22]" );

        assertYieldsErrors( "{ \"statements\" : [ { \"statement\" : \"stmt\", \"parameters\" : [\"AN ARRAY!!\"] } ] }",
                         "Could not map the incoming JSON", "Can not deserialize instance of" +
                                " java.util.LinkedHashMap out of START_ARRAY token\n at [Source: TestInputStream; " +
                                "line: 1, column: 42]" );
    }

    private void assertYieldsErrors( String json, String... expectedErrorMessages )
    {
        StatementDeserializer de = new StatementDeserializer( new ByteArrayInputStream( UTF8.encode( json ) )
        {
            @Override
            public String toString()
            {
                return "TestInputStream";
            }
        } );

        try
        {
            while ( de.read() != null )
            {
            }
            fail( "An exception should have been thrown" );
        }
        catch ( InputFormatException e )
        {
            List<String> errorMessages = new ArrayList<>();
            errorMessages.add( e.getMessage() );

            Throwable t = e;
            while ( true )
            {
                t = t.getCause();

                if ( t == null )
                {
                    break;
                }
                errorMessages.add( t.getMessage() );
            }

            assertThat( errorMessages, equalTo( Arrays.asList( expectedErrorMessages ) ) );
        }
    }
}
