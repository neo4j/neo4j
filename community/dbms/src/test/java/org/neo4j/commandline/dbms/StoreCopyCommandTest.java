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
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class StoreCopyCommandTest
{
    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new StoreCopyCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo( String.format(
                "Copy a database and optionally apply filters.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "copy (--from-database=<database> | --from-path=<path>) [--force] [--verbose]%n" +
                "     [--from-pagecache=<size>] [--from-path-tx=<path>] --to-database=<database>%n" +
                "     [--to-format=<format>] [--to-pagecache=<size>]%n" +
                "     [--delete-nodes-with-labels=<label>[,<label>...]]... [--skip-labels=<label>%n" +
                "     [,<label>...]]... [--skip-properties=<property>[,<property>...]]...%n" +
                "     [--skip-relationships=<relationship>[,<relationship>...]]...%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "This command will create a copy of a database.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose              Enable verbose output.%n" +
                "      --from-database=<database>%n" +
                "                             Name of database to copy from.%n" +
                "      --from-path=<path>     Path to the database to copy from.%n" +
                "      --from-path-tx=<path>  Path to the transaction files, if they are not in%n" +
                "                               the same folder as '--from-path'.%n" +
                "      --to-database=<database>%n" +
                "                             Name of database to copy to.%n" +
                "      --force                Force the command to run even if the integrity of%n" +
                "                               the database can not be verified.%n" +
                "      --to-format=<format>   Set the format for the new database. Must be one%n" +
                "                               of same, standard, high_limit. 'same' will use%n" +
                "                               the same format as the source. WARNING: If you%n" +
                "                               go from 'high_limit' to 'standard' there is no%n" +
                "                               validation that the data will actually fit.%n" +
                "                               Default: same%n" +
                "      --delete-nodes-with-labels=<label>[,<label>...]%n" +
                "                             A comma separated list of labels. All nodes that%n" +
                "                               have ANY of the specified labels will be deleted.%n" +
                "      --skip-labels=<label>[,<label>...]%n" +
                "                             A comma separated list of labels to ignore.%n" +
                "      --skip-properties=<property>[,<property>...]%n" +
                "                             A comma separated list of property keys to ignore.%n" +
                "      --skip-relationships=<relationship>[,<relationship>...]%n" +
                "                             A comma separated list of relationships to ignore.%n" +
                "      --from-pagecache=<size>%n" +
                "                             The size of the page cache to use for reading.%n" +
                "                               Default: 8m%n" +
                "      --to-pagecache=<size>  The size of the page cache to use for writing.%n" +
                "                               Default: 8m"
        ) ) );
    }
}
