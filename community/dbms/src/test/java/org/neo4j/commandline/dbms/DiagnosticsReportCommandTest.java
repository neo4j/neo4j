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
package org.neo4j.commandline.dbms;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;

import static org.assertj.core.api.Assertions.assertThat;

class DiagnosticsReportCommandTest
{
    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new DiagnosticsReportCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Produces a zip/tar of the most common information needed for remote assessments.%n" +
                        "%n" +
                        "USAGE%n" +
                        "%n" +
                        "report [--expand-commands] [--force] [--list] [--verbose]%n" +
                        "       [--database=<database>] [--pid=<pid>] [--to=<path>] [<classifier>...]%n" +
                        "%n" +
                        "DESCRIPTION%n" +
                        "%n" +
                        "Will collect information about the system and package everything in an archive.%n" +
                        "If you specify 'all', everything will be included. You can also fine tune the%n" +
                        "selection by passing classifiers to the tool, e.g 'logs tx threads'.%n" +
                        "%n" +
                        "PARAMETERS%n" +
                        "%n" +
                        "      [<classifier>...]     Default: [config, logs, metrics, plugins, ps,%n" +
                        "                            sysprop, threads, tree, version]%n" +
                        "%n" +
                        "OPTIONS%n" +
                        "%n" +
                        "      --verbose           Enable verbose output.%n" +
                        "      --expand-commands   Allow command expansion in config value evaluation.%n" +
                        "      --database=<database>%n" +
                        "                          Name of the database to report for. Can contain * and%n" +
                        "                            ? for globbing. Note that * and ? have special%n" +
                        "                            meaning in some shells and might need to be escaped%n" +
                        "                            or used with quotes.%n" +
                        "                            Default: *%n" +
                        "      --list              List all available classifiers%n" +
                        "      --force             Ignore disk full warning%n" +
                        "      --to=<path>         Destination directory for reports. Defaults to a%n" +
                        "                            system tmp directory.%n" +
                        "      --pid=<pid>         Specify process id of running neo4j instance"
        ) );
    }
}
