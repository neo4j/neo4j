/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.shell;

import java.io.File;

import org.neo4j.shell.kernel.GraphDatabaseShellServer;

import static org.neo4j.shell.RmiPublicationIT.createDefaultConfigFile;

public class DontShutdownLocalServer
{
    private DontShutdownLocalServer()
    {
    }

    public static void main( String[] args ) throws Exception
    {
        File path = new File( args[0] );
        File configFile = createDefaultConfigFile( path );
        new GraphDatabaseShellServer( path, false, configFile.getAbsolutePath() );
        // Intentionally don't shutdown the server
    }
}
