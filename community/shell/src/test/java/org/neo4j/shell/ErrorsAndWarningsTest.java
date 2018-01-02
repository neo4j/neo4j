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
package org.neo4j.shell;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class ErrorsAndWarningsTest
{
    @Rule
    public ImpermanentDatabaseRule db = null;

    public void startDatabase( final Boolean presentError )
    {
        db = new ImpermanentDatabaseRule()
        {
            @Override
            protected void configure( GraphDatabaseBuilder builder )
            {
                builder.setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE );
                builder.setConfig( GraphDatabaseSettings.cypher_hints_error, presentError.toString() );
            }

            @Override
            public GraphDatabaseService getGraphDatabaseService()
            {
                try
                {
                    before();
                }
                catch ( Throwable throwable )
                {
                    throwable.printStackTrace();
                }
                return super.getGraphDatabaseService();
            }
        };

        db.getGraphDatabaseService();
    }

    @Test
    public void unsupportedQueryShouldPresentWarningIfExplain() throws IOException
    {
        // Given
        // an empty database

        startDatabase( false );

        // When
        InputStream realStdin = System.in;
        try
        {
            System.setIn( new ByteArrayInputStream( "EXPLAIN CYPHER planner=cost CREATE ();".getBytes() ) );
            String output = runAndCaptureOutput( new String[]{"-file", "-"} );

            // Then we should get a warning
            assertThat( output, containsString(
                    "Using COST planner is unsupported for this query, please use RULE planner instead" ) );
        }
        finally
        {
            System.setIn( realStdin );
        }
    }

    @Test
    public void unsupportedQueryShouldBeSilent() throws IOException
    {
        // Given
        // an empty database

        startDatabase( false );

        // When
        InputStream realStdin = System.in;
        try
        {
            System.setIn( new ByteArrayInputStream( "CYPHER planner=cost CREATE ();".getBytes() ) );
            String output = runAndCaptureOutput( new String[]{"-file", "-"} );

            // Then we should not get a warning
            assertThat( output, not( containsString(
                    "Using COST planner is unsupported for this query, please use RULE planner instead" ) ) );
        }
        finally
        {
            System.setIn( realStdin );
        }
    }

    @Test
    public void unsupportedQueryShouldPresentErrorIfConfigured() throws IOException
    {
        // Given
        // an empty database

        startDatabase( true );

        // When
        InputStream realStdin = System.in;
        try
        {
            System.setIn( new ByteArrayInputStream( "CYPHER planner=cost CREATE ();".getBytes() ) );
            String output = runAndCaptureOutput( new String[]{"-file", "-"} );

            // Then we should get an error
            assertThat( output,
                    containsString( "The given query is not currently supported in the selected cost-based planner" ) );
        }
        finally
        {
            System.setIn( realStdin );
        }
    }

    private String runAndCaptureOutput( String[] arguments )
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( buf );
        PrintStream oldOut = System.out;
        System.setOut( out );

        try
        {
            StartClient.main( arguments );
            out.close();
            return buf.toString();
        }
        finally
        {
            System.setOut( oldOut );
        }
    }
}
