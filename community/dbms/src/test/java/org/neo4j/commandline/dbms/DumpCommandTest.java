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

package org.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.dbms.archive.Dumper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DumpCommandTest
{
    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new DumpCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ), mock( Dumper.class ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Dump a database into a single-file archive.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "dump [--verbose] [--database=<database>] --to=<path>%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Dump a database into a single-file archive. The archive can be used by the load%n" +
                "command. <destination-path> can be a file or directory (in which case a file%n" +
                "called <database>.dump will be created). It is not possible to dump a database%n" +
                "that is mounted in a running Neo4j server.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose     Enable verbose output.%n" +
                "      --database=<database>%n" +
                "                    Name of the database to dump.%n" +
                "                      Default: neo4j%n" +
                "      --to=<path>   Destination (file or folder) of database dump."
        ) );
    }
}
