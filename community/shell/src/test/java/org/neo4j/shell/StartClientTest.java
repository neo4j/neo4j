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
package org.neo4j.shell;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Settings;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class StartClientTest
{
    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE );
        }
    };

    @Test
    public void givenShellClientWhenOpenFileThenExecuteFileCommands()
    {
        // Given
        // an empty database

        // When
        StartClient.main(new String[]{"-file", getClass().getResource( "/testshell.txt" ).getFile()});

        // Then
        try ( Transaction tx = db.getGraphDatabaseService().beginTx() )
        {
            assertThat( (String) db.getGraphDatabaseService().getNodeById( 0 ).getProperty( "foo" ),
                    equalTo( "bar" ) );
            tx.success();
        }
    }

    @Test
    public void givenShellClientWhenReadFromStdinThenExecutePipedCommands() throws IOException
    {
        // Given
        // an empty database

        // When
        InputStream realStdin = System.in;
        try
        {
            System.setIn( new ByteArrayInputStream( "CREATE (n {foo:'bar'});".getBytes() ) );
            StartClient.main( new String[] { "-file", "-" } );
        }
        finally
        {
            System.setIn( realStdin );
        }

        // Then
        try ( Transaction tx = db.getGraphDatabaseService().beginTx() )
        {
            assertThat( (String) db.getGraphDatabaseService().getNodeById( 0 ).getProperty( "foo" ),
                    equalTo( "bar" ) );
            tx.success();
        }
    }
}
