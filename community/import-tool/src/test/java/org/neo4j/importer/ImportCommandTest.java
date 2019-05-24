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
package org.neo4j.importer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Set;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.text.IsEmptyString.emptyString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class ImportCommandTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new ImportCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim(), equalTo(
                "USAGE\n" +
                        "\n" +
                        "import [--verbose] [--cache-on-heap[=<true/false>]] [--high-io[=<true/false>]]\n" +
                        "       [--ignore-empty-strings[=<true/false>]] [--ignore-extra-columns\n" +
                        "       [=<true/false>]] [--legacy-style-quoting[=<true/false>]]\n" +
                        "       [--multiline-fields[=<true/false>]] [--normalize-types[=<true/false>]]\n" +
                        "       [--skip-bad-entries-logging[=<true/false>]] [--skip-bad-relationships\n" +
                        "       [=<true/false>]] [--skip-duplicate-nodes[=<true/false>]] [--trim-strings\n" +
                        "       [=<true/false>]] [--additional-config=<path>] [--array-delimiter=<char>]\n" +
                        "       [--bad-tolerance=<num>] [--database=<database>] [--delimiter=<char>]\n" +
                        "       [--id-type=<STRING|INTEGER|ACTUAL>] [--input-encoding=<character-set>]\n" +
                        "       [--max-memory=<size>] [--processors=<num>] [--quote=<char>]\n" +
                        "       [--read-buffer-size=<size>] [--report-file=<path>] --nodes=[<label>[:\n" +
                        "       <label>]...=]<files>... [--nodes=[<label>[:<label>]...=]<files>...]...\n" +
                        "       [--relationships=[<type>=]<files>...]...\n" +
                        "\n" +
                        "DESCRIPTION\n" +
                        "\n" +
                        "Import a collection of CSV files.\n" +
                        "\n" +
                        "OPTIONS\n" +
                        "\n" +
                        "      --verbose              Enable verbose output.\n" +
                        "      --database=<database>  Name of the remote database to backup.\n" +
                        "                               Default: neo4j\n" +
                        "      --additional-config=<path>\n" +
                        "                             Configuration file to supply additional configuration\n" +
                        "                               in.\n" +
                        "      --report-file=<path>   File in which to store the report of the csv-import.\n" +
                        "                               Default: import.report\n" +
                        "      --id-type=<STRING|INTEGER|ACTUAL>\n" +
                        "                             Each node must provide a unique id. This is used to\n" +
                        "                               find the correct nodes when creating relationships.\n" +
                        "                               Possible values are:\n" +
                        "                               STRING: arbitrary strings for identifying nodes,\n" +
                        "                               INTEGER: arbitrary integer values for identifying\n" +
                        "                               nodes,\n" +
                        "                               ACTUAL: (advanced) actual node ids.\n" +
                        "                             For more information on id handling, please see the\n" +
                        "                               Neo4j Manual: https://neo4j.\n" +
                        "                               com/docs/operations-manual/current/tools/import/\n" +
                        "                               Default: STRING\n" +
                        "      --input-encoding=<character-set>\n" +
                        "                             Character set that input data is encoded in.\n" +
                        "                               Default: UTF-8\n" +
                        "      --ignore-extra-columns[=<true/false>]\n" +
                        "                             If un-specified columns should be ignored during the\n" +
                        "                               import.\n" +
                        "                               Default: false\n" +
                        "      --multiline-fields[=<true/false>]\n" +
                        "                             Whether or not fields from input source can span\n" +
                        "                               multiple lines, i.e. contain newline characters.\n" +
                        "                               Default: false\n" +
                        "      --ignore-empty-strings[=<true/false>]\n" +
                        "                             Whether or not empty string fields, i.e. \"\" from input\n" +
                        "                               source are ignored, i.e. treated as null.\n" +
                        "                               Default: false\n" +
                        "      --trim-strings[=<true/false>]\n" +
                        "                             Whether or not strings should be trimmed for\n" +
                        "                               whitespaces.\n" +
                        "                               Default: false\n" +
                        "      --legacy-style-quoting[=<true/false>]\n" +
                        "                             Whether or not backslash-escaped quote e.g. \\\" is\n" +
                        "                               interpreted as inner quote.\n" +
                        "                               Default: false\n" +
                        "      --delimiter=<char>     Delimiter character between values in CSV data.\n" +
                        "                               Default: ,\n" +
                        "      --array-delimiter=<char>\n" +
                        "                             Delimiter character between array elements within a\n" +
                        "                               value in CSV data.\n" +
                        "                               Default: ;\n" +
                        "      --quote=<char>         Character to treat as quotation character for values in\n" +
                        "                               CSV data. Quotes can be escaped as per RFC 4180 by\n" +
                        "                               doubling them, for example \"\" would be interpreted as\n" +
                        "                               a literal \". You cannot escape using \\.\n" +
                        "                               Default: \"\n" +
                        "      --read-buffer-size=<size>\n" +
                        "                             Size of each buffer for reading input data. It has to\n" +
                        "                               at least be large enough to hold the biggest single\n" +
                        "                               value in the input data.\n" +
                        "                               Default: 4194304\n" +
                        "      --max-memory=<size>    Maximum memory that neo4j-admin can use for various\n" +
                        "                               data structures and caching to improve performance.\n" +
                        "                               Values can be plain numbers, like 10000000 or e.g.\n" +
                        "                               20G for 20 gigabyte, or even e.g. 70%.\n" +
                        "                               Default: 90%\n" +
                        "      --high-io[=<true/false>]\n" +
                        "                             Ignore environment-based heuristics, and assume that\n" +
                        "                               the target storage subsystem can support parallel IO\n" +
                        "                               with high throughput.\n" +
                        "                               Default: false\n" +
                        "      --cache-on-heap[=<true/false>]\n" +
                        "                             (advanced) Whether or not to allow allocating memory\n" +
                        "                               for the cache on heap. If 'false' then caches will\n" +
                        "                               still be allocated off-heap, but the additional free\n" +
                        "                               memory inside the JVM will not be allocated for the\n" +
                        "                               caches. This to be able to have better control over\n" +
                        "                               the heap memory\n" +
                        "                               Default: false\n" +
                        "      --processors=<num>     (advanced) Max number of processors used by the\n" +
                        "                               importer. Defaults to the number of available\n" +
                        "                               processors reported by the JVM. There is a certain\n" +
                        "                               amount of minimum threads needed so for that reason\n" +
                        "                               there is no lower bound for this value. For optimal\n" +
                        "                               performance this value shouldn't be greater than the\n" +
                        "                               number of available processors.\n" +
                        "                               Default: 8\n" +
                        "      --bad-tolerance=<num>  Number of bad entries before the import is considered\n" +
                        "                               failed. This tolerance threshold is about\n" +
                        "                               relationships referring to missing nodes. Format\n" +
                        "                               errors in input data are still treated as errors\n" +
                        "                               Default: 1000\n" +
                        "      --skip-bad-entries-logging[=<true/false>]\n" +
                        "                             Whether or not to skip logging bad entries detected\n" +
                        "                               during import.\n" +
                        "                               Default: false\n" +
                        "      --skip-bad-relationships[=<true/false>]\n" +
                        "                             Whether or not to skip importing relationships that\n" +
                        "                               refers to missing node ids, i.e. either start or end\n" +
                        "                               node id/group referring to node that wasn't specified\n" +
                        "                               by the node input data. Skipped nodes will be logged,\n" +
                        "                               containing at most number of entities specified by\n" +
                        "                               bad-tolerance, unless otherwise specified by\n" +
                        "                               skip-bad-entries-logging option.\n" +
                        "                               Default: false\n" +
                        "      --skip-duplicate-nodes[=<true/false>]\n" +
                        "                             Whether or not to skip importing nodes that have the\n" +
                        "                               same id/group. In the event of multiple nodes within\n" +
                        "                               the same group having the same id, the first\n" +
                        "                               encountered will be imported whereas consecutive such\n" +
                        "                               nodes will be skipped. Skipped nodes will be logged,\n" +
                        "                               containing at most number of entities specified by\n" +
                        "                               bad-tolerance, unless otherwise specified by\n" +
                        "                               skip-bad-entries-logging option.\n" +
                        "                               Default: false\n" +
                        "      --normalize-types[=<true/false>]\n" +
                        "                             Whether or not to normalize property types to Cypher\n" +
                        "                               types, e.g. 'int' becomes 'long' and 'float' becomes\n" +
                        "                               'double'\n" +
                        "                               Default: true\n" +
                        "      --nodes=[<label>[:<label>]...=]<files>...\n" +
                        "                             Node CSV header and data. Multiple files will be\n" +
                        "                               logically seen as one big file from the perspective\n" +
                        "                               of the importer. The first line must contain the\n" +
                        "                               header. Multiple data sources like these can be\n" +
                        "                               specified in one import, where each data source has\n" +
                        "                               its own header.\n" +
                        "      --relationships=[<type>=]<files>...\n" +
                        "                             Relationship CSV header and data. Multiple files will\n" +
                        "                               be logically seen as one big file from the\n" +
                        "                               perspective of the importer. The first line must\n" +
                        "                               contain the header. Multiple data sources like these\n" +
                        "                               can be specified in one import, where each data\n" +
                        "                               source has its own header."
        ) );
    }

    @Test
    void shouldKeepSpecifiedNeo4jHomeWhenAdditionalConfigIsPresent()
    {
        // given
        final var homeDir = testDir.directory( "other", "place" );
        final var additionalConfigFile = testDir.createFile( "empty.conf" );
        final var ctx = new ExecutionContext( homeDir.toPath(), testDir.directory( "conf" ).toPath(), System.out, System.err, testDir.getFileSystem() );
        final var command = new ImportCommand( ctx );
        final var foo = testDir.createFile( "foo.csv" );

        CommandLine.populateCommand( command, "--additional-config", additionalConfigFile.getAbsolutePath(), "--nodes=" + foo.getAbsolutePath() );

        // when
        Config resultingConfig = command.loadNeo4jConfig();

        // then
        assertEquals( homeDir, resultingConfig.get( GraphDatabaseSettings.neo4j_home ) );
    }

    @Test
    void shouldKeepSpecifiedNeo4jHomeWhenNoAdditionalConfigIsPresent()
    {
        // given
        final var homeDir = testDir.directory( "other", "place" );
        final var ctx = new ExecutionContext( homeDir.toPath(), testDir.directory( "conf" ).toPath(), System.out, System.err, testDir.getFileSystem() );
        final var command = new ImportCommand( ctx );
        final var foo = testDir.createFile( "foo.csv" );

        CommandLine.populateCommand( command, "--nodes=" + foo.getAbsolutePath() );

        // when
        Config resultingConfig = command.loadNeo4jConfig();

        // then
        assertEquals( homeDir, resultingConfig.get( GraphDatabaseSettings.neo4j_home ) );
    }

    @Nested
    class ParseNodeFilesGroup
    {
        @Test
        void illegalEqualsPosition()
        {
            assertThrows( IllegalArgumentException.class, () -> ImportCommand.parseNodeFilesGroup( "=foo.csv" ) );
            assertThrows( IllegalArgumentException.class, () -> ImportCommand.parseNodeFilesGroup( "foo=" ) );
        }

        @Test
        void validateFileExistence()
        {
            assertThrows( IllegalArgumentException.class, () -> ImportCommand.parseRelationshipFilesGroup( "nonexisting.file" ) );
        }

        @Test
        void filesWithoutLabels()
        {
            final var foo = testDir.createFile( "foo.csv" );
            final var bar = testDir.createFile( "bar.csv" );
            final var g = ImportCommand.parseNodeFilesGroup( foo.getPath() + "," + bar.getPath() );
            assertThat( g.key, empty() );
            assertThat( g.files, arrayContaining( foo, bar ) );
        }

        @Test
        void singleLabel()
        {
            final var foo = testDir.createFile( "foo.csv" );
            final var bar = testDir.createFile( "bar.csv" );
            final var g = ImportCommand.parseNodeFilesGroup( "BANANA=" + foo.getPath() + "," + bar.getPath() );
            assertThat( g.key, equalTo( Set.of( "BANANA" ) ) );
            assertThat( g.files, arrayContaining( foo, bar ) );
        }

        @Test
        void multipleLabels()
        {
            final var foo = testDir.createFile( "foo.csv" );
            final var bar = testDir.createFile( "bar.csv" );
            final var g = ImportCommand.parseNodeFilesGroup( ":APPLE::KIWI : BANANA=" + foo.getPath() + "," + bar.getPath() );
            assertThat( g.key, equalTo( Set.of( "BANANA", "KIWI", "APPLE" ) ) );
            assertThat( g.files, arrayContaining( foo, bar ) );
        }

        @Test
        void filesRegex()
        {
            final var foo1 = testDir.createFile( "foo-1.csv" );
            final var foo2 = testDir.createFile( "foo-2.csv" );
            final var foo3 = testDir.createFile( "foo-X.csv" );
            final var g = ImportCommand.parseNodeFilesGroup( "BANANA=" + testDir.absolutePath() + File.separator + "foo\\-\\d\\.csv" );
            assertThat( g.key, equalTo( Set.of( "BANANA" ) ) );
            assertThat( g.files, arrayContaining( foo1, foo2 ) );
        }
    }

    @Nested
    class ParseRelationshipFilesGroup
    {
        @Test
        void illegalEqualsPosition()
        {
            assertThrows( IllegalArgumentException.class, () -> ImportCommand.parseRelationshipFilesGroup( "=foo.csv" ) );
            assertThrows( IllegalArgumentException.class, () -> ImportCommand.parseRelationshipFilesGroup( "foo=" ) );
        }

        @Test
        void validateFileExistence()
        {
            assertThrows( IllegalArgumentException.class, () -> ImportCommand.parseRelationshipFilesGroup( "nonexisting.file" ) );
        }

        @Test
        void filesWithoutLabels()
        {
            final var foo = testDir.createFile( "foo.csv" );
            final var bar = testDir.createFile( "bar.csv" );
            final var g = ImportCommand.parseRelationshipFilesGroup( foo.getPath() + "," + bar.getPath() );
            assertThat( g.key, emptyString() );
            assertThat( g.files, arrayContaining( foo, bar ) );
        }

        @Test
        void withDefaultRelType()
        {
            final var foo = testDir.createFile( "foo.csv" );
            final var bar = testDir.createFile( "bar.csv" );
            final var g = ImportCommand.parseRelationshipFilesGroup( "BANANA=" + foo.getPath() + "," + bar.getPath() );
            assertThat( g.key, equalTo( "BANANA" ) );
            assertThat( g.files, arrayContaining( foo, bar ) );
        }

        @Test
        void filesRegex()
        {
            final var foo1 = testDir.createFile( "foo-1.csv" );
            final var foo2 = testDir.createFile( "foo-2.csv" );
            final var foo3 = testDir.createFile( "foo-X.csv" );
            final var g = ImportCommand.parseRelationshipFilesGroup( "BANANA=" + testDir.absolutePath() + File.separator + "foo\\-\\d\\.csv" );
            assertThat( g.key, equalTo( "BANANA" ) );
            assertThat( g.files, arrayContaining( foo1, foo2 ) );
        }
    }
}
