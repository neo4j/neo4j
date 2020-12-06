/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell.commands;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.ShellParameterMap;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.Encryption;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.prettyprint.PrettyConfig;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.DEFAULT_DEFAULT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.SYSTEM_DB_NAME;
import static org.neo4j.shell.util.Versions.majorVersion;

public class CypherShellMultiDatabaseIntegrationTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private StringLinePrinter linePrinter = new StringLinePrinter();
    private Command useCommand;
    private Command beginCommand;
    private Command rollbackCommand;
    private CypherShell shell;

    @Before
    public void setUp() throws Exception
    {
        linePrinter.clear();
        shell = new CypherShell( linePrinter, new PrettyConfig( Format.PLAIN, true, 1000 ), false, new ShellParameterMap() );
        useCommand = new Use( shell );
        beginCommand = new Begin( shell );
        rollbackCommand = new Rollback( shell );

        shell.connect( new ConnectionConfig( "bolt", "localhost", 7687, "neo4j", "neo", Encryption.DEFAULT, ABSENT_DB_NAME ) );

        // Multiple databases are only available from 4.0
        assumeTrue( majorVersion( shell.getServerVersion() ) >= 4 );
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void switchingToSystemDatabaseWorks() throws CommandException
    {
        useCommand.execute( SYSTEM_DB_NAME );

        assertThat( linePrinter.output(), is( "" ) );
        assertOnSystemDB();
    }

    @Test
    public void switchingToSystemDatabaseIsNotCaseSensitive() throws CommandException
    {
        useCommand.execute( "SyStEm" );

        assertThat( linePrinter.output(), is( "" ) );
        assertOnSystemDB();
    }

    @Test
    public void switchingToSystemDatabaseAndBackToNeo4jWorks() throws CommandException
    {
        useCommand.execute( SYSTEM_DB_NAME );
        useCommand.execute( DEFAULT_DEFAULT_DB_NAME );

        assertThat( linePrinter.output(), is( "" ) );
        assertOnRegularDB();
    }

    @Test
    public void switchingToSystemDatabaseAndBackToDefaultWorks() throws CommandException
    {
        useCommand.execute( SYSTEM_DB_NAME );
        useCommand.execute( ABSENT_DB_NAME );

        assertThat( linePrinter.output(), is( "" ) );
        assertOnRegularDB();
    }

    @Test
    public void switchingDatabaseInOpenTransactionShouldFail() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( "There is an open transaction." );

        beginCommand.execute( "" );
        useCommand.execute( "another_database" );
    }

    @Test
    public void switchingDatabaseAfterRollbackTransactionWorks() throws CommandException
    {
        beginCommand.execute( "" );
        rollbackCommand.execute( "" );
        useCommand.execute( SYSTEM_DB_NAME );

        assertThat( linePrinter.output(), is( "" ) );
        assertOnSystemDB();
    }

    @Test
    public void switchingToNonExistingDatabaseShouldGiveErrorResponseFromServer() throws CommandException
    {
        useCommand.execute( SYSTEM_DB_NAME );

        try
        {
            useCommand.execute( "this_database_name_does_not_exist_in_test_container" );
            fail( "No ClientException thrown" );
        }
        catch ( ClientException e )
        {
            // In non-interactive we want to switch even if the database does not exist (in case we don't have fail-fast)
            assertOnNoValidDB();
        }
    }

    @Test
    public void switchingToNonExistingDatabaseShouldGiveErrorResponseFromServerInteractive() throws CommandException
    {
        shell = new CypherShell( linePrinter, new PrettyConfig( Format.PLAIN, true, 1000 ), true, new ShellParameterMap() );
        useCommand = new Use( shell );
        shell.connect( new ConnectionConfig( "bolt", "localhost", 7687, "neo4j", "neo", Encryption.DEFAULT, ABSENT_DB_NAME ) );

        useCommand.execute( SYSTEM_DB_NAME );

        try
        {
            useCommand.execute( "this_database_name_does_not_exist_in_test_container" );
            fail( "No ClientException thrown" );
        }
        catch ( ClientException e )
        {
            // In interactive we do not want to switch if the database does not exist
            assertOnSystemDB();
        }
    }

    // HELPERS

    private void assertOnRegularDB() throws CommandException
    {
        shell.execute( "RETURN 'toadstool'" );
        assertThat( linePrinter.output(), containsString( "toadstool" ) );
    }

    private void assertOnSystemDB() throws CommandException
    {
        shell.execute( "SHOW DATABASES" );
        assertThat( linePrinter.output(), containsString( "neo4j" ) );
        assertThat( linePrinter.output(), containsString( "system" ) );
    }

    private void assertOnNoValidDB() throws CommandException
    {
        thrown.expect( ClientException.class );
        thrown.expectMessage( "Database does not exist" );
        shell.execute( "RETURN 1" );
    }
}
