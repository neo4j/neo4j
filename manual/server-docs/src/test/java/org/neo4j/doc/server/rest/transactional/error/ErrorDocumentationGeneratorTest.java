/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest.transactional.error;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.neo4j.server.rest.transactional.error.ErrorDocumentationGenerator;

import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;

public class ErrorDocumentationGeneratorTest
{
    @Test
    public void tablesShouldFormatAsAsciiDoc() throws Exception
    {
        // Given
        ErrorDocumentationGenerator.Table table = new ErrorDocumentationGenerator.Table();
        table.setCols( "COLS" );
        table.setHeader( "A", "B" );
        table.addRow( 1, 2 );
        table.addRow( 3, 4 );

        // When
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( buf, false, StandardCharsets.UTF_8.name() );
        table.print( out );
        out.flush();

        // Then
        String result = buf.toString( StandardCharsets.UTF_8.name() );
        String n = System.lineSeparator();
        String expected =
                "[options=\"header\", cols=\"COLS\"]" + n +
                "|===" + n +
                "|A |B " + n +
                "|1 |2 " + n +
                "|3 |4 " + n +
                "|===" + n;
        assertThat( result, is(equalTo( expected )) );
    }


    @Test
    public void shouldGenerateTableOfClassifications() throws Exception
    {
        // Given
        ErrorDocumentationGenerator gen = new ErrorDocumentationGenerator();

        // When
        ErrorDocumentationGenerator.Table table = gen.generateClassificationDocs();

        // Then
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        table.print( new PrintStream( buf, true, StandardCharsets.UTF_8.name() ) );
        String actual = buf.toString( StandardCharsets.UTF_8.name() );

        // More or less randomly chosen bits of text that should be in the output:
        assertThat( actual, stringContainsInOrder( asList( "DatabaseError", "Rollback" ) ) );
    }

    @Test
    public void shouldGenerateTableOfStatusCodes() throws Exception
    {
        // Given
        ErrorDocumentationGenerator gen = new ErrorDocumentationGenerator();

        // When
        ErrorDocumentationGenerator.Table table = gen.generateStatusCodeDocs();

        // Then
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        table.print( new PrintStream( buf, true, StandardCharsets.UTF_8.name() ) );
        String actual = buf.toString( StandardCharsets.UTF_8.name() );

        // More or less randomly chosen bits of text that should be in the output:
        assertThat( actual, stringContainsInOrder( asList( "UnknownFailure", "An unknown failure occurred" ) ) );
    }
}
