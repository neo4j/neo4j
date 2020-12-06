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

import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;

abstract class CypherShellIntegrationTest
{
    CypherShell shell;

    void connect( String password ) throws CommandException
    {
        // Try with encryption first, which is the default for 3.X
        try
        {
            shell.connect( new ConnectionConfig( "bolt://", "localhost", 7687, "neo4j", password, true ) );
        }
        catch ( ServiceUnavailableException e )
        {
            // This means we are probablyin 4.X, let's retry with encryption off
            shell.connect( new ConnectionConfig( "bolt://", "localhost", 7687, "neo4j", password, false ) );
        }
    }
}
