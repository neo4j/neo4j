/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.shell.CypherShell;
import org.neo4j.shell.ShellParameterMap;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.state.ErrorWhileInTransactionException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CypherShellTransactionIntegrationTest extends CypherShellIntegrationTest
{
    private final StringLinePrinter linePrinter = new StringLinePrinter();
    private Command rollbackCommand;
    private Command commitCommand;
    private Command beginCommand;

    @BeforeEach
    void setUp() throws Exception
    {
        shell = new CypherShell( linePrinter, new PrettyConfig( Format.VERBOSE, true, 1000 ), false, new ShellParameterMap() );
        rollbackCommand = new Rollback( shell );
        commitCommand = new Commit( shell );
        beginCommand = new Begin( shell );

        connect( "neo" );
        shell.execute( "MATCH (n) DETACH DELETE (n)" );
    }

    @AfterEach
    void cleanUp()
    {
        shell.disconnect();
        linePrinter.clear();
    }

    @Test
    void rollbackScenario() throws CommandException
    {
        //given
        shell.execute( "CREATE (:TestPerson {name: \"Jane Smith\"})" );

        //when
        beginCommand.execute( "" );
        shell.execute( "CREATE (:NotCreated)" );
        rollbackCommand.execute( "" );

        //then
        shell.execute( "MATCH (n) RETURN n" );

        String output = linePrinter.output();
        assertThat( output, containsString( "| n " ) );
        assertThat( output, containsString( "| (:TestPerson {name: \"Jane Smith\"}) |" ) );
        assertThat( output, not( containsString( ":NotCreated" ) ) );
    }

    @Test
    void failureInTxScenario() throws CommandException
    {
        // given
        beginCommand.execute( "" );

        // then
        ErrorWhileInTransactionException exception = assertThrows( ErrorWhileInTransactionException.class, () -> shell.execute( "RETURN 1/0" ) );
        assertThat( exception.getMessage(), containsString( "/ by zero" ) );
        assertThat( exception.getMessage(),
                containsString( "An error occurred while in an open transaction. The transaction will be rolled back and terminated." ) );
    }

    @Test
    void failureInTxScenarioWithCypherFollowing() throws CommandException
    {
        // given
        beginCommand.execute( "" );
        try
        {
            shell.execute( "RETURN 1/0" );
        }
        catch ( ErrorWhileInTransactionException ignored )
        {
            // This is OK
        }

        // when
        shell.execute( "RETURN 42" );

        // then
        assertThat( linePrinter.output(), containsString( "42" ) );
    }

    @Test
    void failureInTxScenarioWithCommitFollowing() throws CommandException
    {
        // given
        beginCommand.execute( "" );
        try
        {
            shell.execute( "RETURN 1/0" );
        }
        catch ( ErrorWhileInTransactionException ignored )
        {
            // This is OK
        }

        // then / when
        CommandException exception = assertThrows( CommandException.class, () -> commitCommand.execute( "" ) );
        assertThat( exception.getMessage(), containsString( "There is no open transaction to commit" ) );
    }

    @Test
    void failureInTxScenarioWithRollbackFollowing() throws CommandException
    {
        // given
        beginCommand.execute( "" );
        try
        {
            shell.execute( "RETURN 1/0" );
        }
        catch ( ErrorWhileInTransactionException ignored )
        {
            // This is OK
        }

        //  then / when
        CommandException exception = assertThrows( CommandException.class, () -> rollbackCommand.execute( "" ) );
        assertThat( exception.getMessage(), containsString( "There is no open transaction to rollback" ) );
    }

    @Test
    void resetInFailedTxScenario() throws CommandException
    {
        //when
        beginCommand.execute( "" );
        try
        {
            shell.execute( "RETURN 1/0" );
        }
        catch ( ErrorWhileInTransactionException ignored )
        {
            // This is OK
        }
        shell.reset();

        //then
        shell.execute( "CREATE (:TestPerson {name: \"Jane Smith\"})" );
        shell.execute( "MATCH (n) RETURN n" );

        String result = linePrinter.output();
        assertThat( result, containsString( "| (:TestPerson {name: \"Jane Smith\"}) |" ) );
        assertThat( result, not( containsString( ":NotCreated" ) ) );
    }

    @Test
    void resetInTxScenario() throws CommandException
    {
        //when
        beginCommand.execute( "" );
        shell.execute( "CREATE (:NotCreated)" );
        shell.reset();

        //then
        shell.execute( "CREATE (:TestPerson {name: \"Jane Smith\"})" );
        shell.execute( "MATCH (n) RETURN n" );

        String result = linePrinter.output();
        assertThat( result, containsString( "| (:TestPerson {name: \"Jane Smith\"}) |" ) );
        assertThat( result, not( containsString( ":NotCreated" ) ) );
    }

    @Test
    void commitScenario() throws CommandException
    {
        beginCommand.execute( "" );
        shell.execute( "CREATE (:TestPerson {name: \"Joe Smith\"})" );
        assertThat( linePrinter.output(), containsString( "0 rows\n" ) );

        linePrinter.clear();
        shell.execute( "CREATE (:TestPerson {name: \"Jane Smith\"})" );
        assertThat( linePrinter.output(), containsString( "0 rows\n" ) );

        linePrinter.clear();
        shell.execute( "MATCH (n:TestPerson) RETURN n ORDER BY n.name" );
        assertThat( linePrinter.output(), containsString( "\n| (:TestPerson {name: \"Jane Smith\"}) |\n| (:TestPerson {name: \"Joe Smith\"})  |\n" ) );

        commitCommand.execute( "" );
    }
}
