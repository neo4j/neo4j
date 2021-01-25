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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.exception.CommandException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class UseTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private DatabaseManager mockShell = mock( DatabaseManager.class );
    private Command cmd;

    @Before
    public void setup()
    {
        this.cmd = new Use( mockShell );
    }

    @Test
    public void setAbsentDatabaseOnNoArgument() throws CommandException
    {
        cmd.execute( "" );

        verify( mockShell ).setActiveDatabase( DatabaseManager.ABSENT_DB_NAME );
    }

    @Test
    public void shouldFailIfMoreThanOneArg() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "db1 db2" );
        fail( "Expected error" );
    }

    @Test
    public void setActiveDatabase() throws CommandException
    {
        cmd.execute( "db1" );

        verify( mockShell ).setActiveDatabase( "db1" );
    }

    @Test
    public void printUsage() throws CommandException
    {
        String usage = cmd.getUsage();
        assertEquals( usage, "database" );
    }
}
