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
package org.neo4j.logging;

import org.junit.Test;
import org.neo4j.function.Suppliers;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FormattedLogProviderTest
{
    private static final Date FIXED_DATE = new Date( 467612604343L );

    @Test
    public void shouldReturnSameLoggerForSameClass()
    {
        // Given
        FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream( new ByteArrayOutputStream() );

        // Then
        FormattedLog log = logProvider.getLog( getClass() );
        assertThat( logProvider.getLog( FormattedLogProviderTest.class ), sameInstance( log ) );
    }

    @Test
    public void shouldReturnSameLoggerForSameContext()
    {
        // Given
        FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream( new ByteArrayOutputStream() );

        // Then
        FormattedLog log = logProvider.getLog( "test context" );
        assertThat( logProvider.getLog( "test context" ), sameInstance( log ) );
    }

    @Test
    public void shouldLogWithAbbreviatedClassNameAsContext() throws Exception
    {
        // Given
        StringWriter writer = new StringWriter();
        FormattedLogProvider logProvider = newFormattedLogProvider( writer );
        FormattedLog log = logProvider.getLog( StringWriter.class );

        // When
        log.info( "Terminator 2" );

        // Then
        assertThat( writer.toString(), equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [j.i.StringWriter] Terminator 2%n" ) ) );
    }

    @Test
    public void shouldSetLevelForLogWithMatchingContext() throws Exception
    {
        // Given
        StringWriter writer = new StringWriter();
        FormattedLogProvider logProvider = newFormattedLogProvider( writer, "java.io.StringWriter", Level.DEBUG );

        // When
        FormattedLog stringWriterLog = logProvider.getLog( StringWriter.class );
        FormattedLog otherClassLog = logProvider.getLog( PrintWriter.class );
        FormattedLog matchingNamedLog = logProvider.getLog( "java.io.StringWriter" );
        FormattedLog nonMatchingNamedLog = logProvider.getLog( "java.io.Foo" );

        // Then
        assertThat( stringWriterLog.isDebugEnabled(), is( true ) );
        assertThat( otherClassLog.isDebugEnabled(), is( false ) );
        assertThat( matchingNamedLog.isDebugEnabled(), is( true ) );
        assertThat( nonMatchingNamedLog.isDebugEnabled(), is( false ) );
    }

    @Test
    public void shouldSetLevelForLogWithPartiallyMatchingContext() throws Exception
    {
        // Given
        StringWriter writer = new StringWriter();
        FormattedLogProvider logProvider = newFormattedLogProvider( writer, "java.io", Level.DEBUG );

        // When
        FormattedLog stringWriterLog = logProvider.getLog( StringWriter.class );
        FormattedLog printWriterLog = logProvider.getLog( PrintWriter.class );
        FormattedLog otherClassLog = logProvider.getLog( Date.class );
        FormattedLog matchingNamedLog = logProvider.getLog( "java.io.Foo" );
        FormattedLog nonMatchingNamedLog = logProvider.getLog( "java.util.Foo" );

        // Then
        assertThat( stringWriterLog.isDebugEnabled(), is( true ) );
        assertThat( printWriterLog.isDebugEnabled(), is( true ) );
        assertThat( otherClassLog.isDebugEnabled(), is( false ) );
        assertThat( matchingNamedLog.isDebugEnabled(), is( true ) );
        assertThat( nonMatchingNamedLog.isDebugEnabled(), is( false ) );
    }

    private static FormattedLogProvider newFormattedLogProvider( StringWriter writer )
    {
        return newFormattedLogProvider( writer, Collections.<String, Level>emptyMap() );
    }

    private static FormattedLogProvider newFormattedLogProvider( StringWriter writer, String context, Level level )
    {
        return newFormattedLogProvider( writer, Collections.singletonMap( context, level ) );
    }

    private static FormattedLogProvider newFormattedLogProvider( StringWriter writer, Map<String, Level> levels )
    {
        return new FormattedLogProvider(
                Suppliers.singleton( FIXED_DATE ), Suppliers.singleton( new PrintWriter( writer ) ),
                FormattedLog.UTC, true, levels, Level.INFO, true );
    }
}
