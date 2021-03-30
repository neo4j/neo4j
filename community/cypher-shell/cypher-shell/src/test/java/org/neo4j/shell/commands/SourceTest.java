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

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;

import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parser.ShellStatementParser;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SourceTest
{
    private Source cmd;
    private CypherShell shell;

    @Before
    public void setup()
    {
        shell = mock( CypherShell.class );
        cmd = new Source( shell, new ShellStatementParser() );
    }

    @Test
    public void descriptionNotNull()
    {
        assertNotNull( cmd.getDescription() );
    }

    @Test
    public void usageNotNull()
    {
        assertNotNull( cmd.getUsage() );
    }

    @Test
    public void helpNotNull()
    {
        assertNotNull( cmd.getHelp() );
    }

    @Test
    public void runCommand() throws CommandException
    {
        // given
        cmd.execute( fileFromResource( "test.cypher" ) );
        verify( shell ).execute( "RETURN 42;" );
        verifyNoMoreInteractions( shell );
    }

    @Test
    public void shouldFailIfFileNotThere()
    {
        var exception = assertThrows( CommandException.class, () -> cmd.execute( "not.there" ) );
        assertThat( exception.getMessage(), containsString( "Cannot find file: 'not.there'" ) );
        assertThat( exception.getCause(), instanceOf( FileNotFoundException.class ));
    }

    @Test
    public void shouldNotAcceptMoreThanOneArgs()
    {
        var exception = assertThrows( CommandException.class, () -> cmd.execute( "bob sob" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    public void shouldNotAcceptZeroArgs()
    {
        var exception = assertThrows( CommandException.class, () -> cmd.execute( "" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    public void shouldTryToExecuteIncompleteStatements() throws CommandException
    {
        cmd.execute( fileFromResource( "invalid.cypher" ) );
        verify( shell ).execute( "INVALID CYPHER\nWITHOUT SEMICOLON" );
        verifyNoMoreInteractions( shell );
    }

    private String fileFromResource( String filename )
    {
        return getClass().getResource( filename ).getFile();
    }
}
