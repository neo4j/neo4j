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
package org.neo4j.shell.cli;

import org.junit.jupiter.api.Test;

import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Historian;
import org.neo4j.shell.ShellParameterMap;
import org.neo4j.shell.commands.Begin;
import org.neo4j.shell.commands.Command;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.AnsiLogger;
import org.neo4j.shell.prettyprint.PrettyConfig;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.shell.commands.CommandHelper.simpleArgParse;

class CommandHelperTest
{

    @Test
    void emptyStringIsNoArgs() throws CommandException
    {
        assertEquals( 0, simpleArgParse( "", 0, "", "" ).length );
    }

    @Test
    void whitespaceStringIsNoArgs() throws CommandException
    {
        assertEquals( 0, simpleArgParse( "    \t  ", 0, "", "" ).length );
    }

    @Test
    void oneArg()
    {
        CommandException exception = assertThrows( CommandException.class, () -> simpleArgParse( "bob", 0, "", "" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void shouldIgnoreCaseForCommands()
    {
        // Given
        AnsiLogger logger = new AnsiLogger( false );
        CommandHelper commandHelper =
                new CommandHelper( logger, Historian.empty, new CypherShell( logger, PrettyConfig.DEFAULT, false, new ShellParameterMap() ), null, null );

        // When
        Command begin = commandHelper.getCommand( ":BEGIN" );

        // Then
        assertTrue( begin instanceof Begin );
    }
}
