/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.admin.security;

import org.junit.Before;
import org.junit.Test;

public class UsersCommandIT extends UsersCommandTestBase
{
    @Before
    public void setup()
    {
        super.setup();
        // the following line ensures that the test setup code (like creating test users) works on the same initial
        // environment that the actual tested commands will encounter. In particular some auth state is created
        // on demand in both the UserCommand and in the real server. We want that state created before the tests
        // are run.
        tool.execute( graphDir.toPath(), confDir.toPath(), makeArgs( "users", "list" ) );
        resetOutsideWorldMock();
    }

    @Test
    public void shouldGetUsageErrorsWithNoSubCommand() throws Throwable
    {
        // When running 'users' with no subcommand, expect usage errors
        assertFailedSubCommand( "users", "", new String[0], "Missing arguments: expected sub-command argument 'set-password'",
                "neo4j-admin users <subcommand> [<username>] [<password>]",
                "Sets the initial (admin) user." );
    }
}
