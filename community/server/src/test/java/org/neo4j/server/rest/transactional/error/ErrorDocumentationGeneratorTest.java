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
package org.neo4j.server.rest.transactional.error;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

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
        PrintStream out = new PrintStream( buf, false, "UTF-8" );
        table.print( out );
        out.flush();

        // Then
        String result = buf.toString( "UTF-8" );
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
        table.print( new PrintStream( buf, true, "UTF-8" ) );
        String actual = buf.toString( "UTF-8" );

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
        table.print( new PrintStream( buf, true, "UTF-8" ) );
        String actual = buf.toString( "UTF-8" );

        // More or less randomly chosen bits of text that should be in the output:
        assertThat( actual, stringContainsInOrder( asList( "UnknownFailure", "An unknown failure occurred" ) ) );
    }
}
