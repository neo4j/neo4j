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
package org.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public class AdminToolTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldPrintEnvironmentVariablesInHelpUsage()
    {
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
        try ( PrintStream out = new PrintStream( outBuffer );
                PrintStream err = new PrintStream( errBuffer ) )
        {
            assertEquals( 2, AdminTool.execute(
                    new ExecutionContext( directory.homeDir().toPath(), directory.directory( "conf" ).toPath(), out, err, directory.getFileSystem() ) ) );
        }
        String outString = outBuffer.toString();
        assertTrue( outString.contains( "Environment variables" ) );
        assertTrue( outString.contains( "NEO4J_HOME" ) );
        assertTrue( outString.contains( "NEO4J_CONF" ) );
    }
}
