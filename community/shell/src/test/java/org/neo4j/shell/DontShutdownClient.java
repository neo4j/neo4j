/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.Serializable;
import java.util.HashMap;

import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.impl.SystemOutput;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

public class DontShutdownClient
{
    public static void main( String[] args ) throws Exception
    {
        GraphDatabaseShellServer server = new GraphDatabaseShellServer( args[0], false, null );
        new SameJvmClient( new HashMap<String, Serializable>(), server,
            /* Temporary, switch back to SilentOutput once flaky test is resolved. */ new SystemOutput() );
        server.shutdown();
        // Intentionally don't shutdown the client
    }
}
