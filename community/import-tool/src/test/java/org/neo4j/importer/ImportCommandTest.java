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
package org.neo4j.importer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
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
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.lineSeparator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.text.IsEmptyString.emptyString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
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
        assertEquals( baos.toString().trim(),
                "USAGE" + lineSeparator() +
                        "" + lineSeparator() +
                        "import [--verbose] [--cache-on-heap[=<true/false>]] [--high-io[=<true/false>]]" + lineSeparator() +
                        "       [--ignore-empty-strings[=<true/false>]] [--ignore-extra-columns" + lineSeparator() +
                        "       [=<true/false>]] [--legacy-style-quoting[=<true/false>]]" + lineSeparator() +
                        "       [--multiline-fields[=<true/false>]] [--normalize-types[=<true/false>]]" + lineSeparator() +
                        "       [--skip-bad-entries-logging[=<true/false>]] [--skip-bad-relationships" + lineSeparator() +
                        "       [=<true/false>]] [--skip-duplicate-nodes[=<true/false>]] [--trim-strings" + lineSeparator() +
                        "       [=<true/false>]] [--additional-config=<path>] [--array-delimiter=<char>]" + lineSeparator() +
                        "       [--bad-tolerance=<num>] [--database=<database>] [--delimiter=<char>]" + lineSeparator() +
                        "       [--id-type=<STRING|INTEGER|ACTUAL>] [--input-encoding=<character-set>]" + lineSeparator() +
                        "       [--max-memory=<size>] [--processors=<num>] [--quote=<char>]" + lineSeparator() +
                        "       [--read-buffer-size=<size>] [--report-file=<path>] --nodes=[<label>[:" + lineSeparator() +
                        "       <label>]...=]<files>... [--nodes=[<label>[:<label>]...=]<files>...]..." + lineSeparator() +
                        "       [--relationships=[<type>=]<files>...]..." + lineSeparator() +
                        "" + lineSeparator() +
                        "DESCRIPTION" + lineSeparator() +
                        "" + lineSeparator() +
                        "Import a collection of CSV files." + lineSeparator() +
                        "" + lineSeparator() +
                        "OPTIONS" + lineSeparator() +
                        "" + lineSeparator() +
                        "      --verbose              Enable verbose output." + lineSeparator() +
                        "      --database=<database>  Name of the database to import." + lineSeparator() +
                        "                               Default: neo4j" + lineSeparator() +
                        "      --additional-config=<path>" + lineSeparator() +
                        "                             Configuration file to supply additional" + lineSeparator() +
                        "                               configuration in." + lineSeparator() +
                        "      --report-file=<path>   File in which to store the report of the" + lineSeparator() +
                        "                               csv-import." + lineSeparator() +
                        "                               Default: import.report" + lineSeparator() +
                        "      --id-type=<STRING|INTEGER|ACTUAL>" + lineSeparator() +
                        "                             Each node must provide a unique id. This is used" + lineSeparator() +
                        "                               to find the correct nodes when creating" + lineSeparator() +
                        "                               relationships. Possible values are:" + lineSeparator() +
                        "                               STRING: arbitrary strings for identifying nodes," + lineSeparator() +
                        "                               INTEGER: arbitrary integer values for" + lineSeparator() +
                        "                               identifying nodes," + lineSeparator() +
                        "                               ACTUAL: (advanced) actual node ids." + lineSeparator() +
                        "                             For more information on id handling, please see" + lineSeparator() +
                        "                               the Neo4j Manual: https://neo4j." + lineSeparator() +
                        "                               com/docs/operations-manual/4.0/tools/import/" + lineSeparator() +
                        "                               Default: STRING" + lineSeparator() +
                        "      --input-encoding=<character-set>" + lineSeparator() +
                        "                             Character set that input data is encoded in." + lineSeparator() +
                        "                               Default: UTF-8" + lineSeparator() +
                        "      --ignore-extra-columns[=<true/false>]" + lineSeparator() +
                        "                             If un-specified columns should be ignored during" + lineSeparator() +
                        "                               the import." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --multiline-fields[=<true/false>]" + lineSeparator() +
                        "                             Whether or not fields from input source can span" + lineSeparator() +
                        "                               multiple lines, i.e. contain newline characters." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --ignore-empty-strings[=<true/false>]" + lineSeparator() +
                        "                             Whether or not empty string fields, i.e. \"\" from" + lineSeparator() +
                        "                               input source are ignored, i.e. treated as null." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --trim-strings[=<true/false>]" + lineSeparator() +
                        "                             Whether or not strings should be trimmed for" + lineSeparator() +
                        "                               whitespaces." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --legacy-style-quoting[=<true/false>]" + lineSeparator() +
                        "                             Whether or not backslash-escaped quote e.g. \\\" is" + lineSeparator() +
                        "                               interpreted as inner quote." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --delimiter=<char>     Delimiter character between values in CSV data." + lineSeparator() +
                        "                               Default: ," + lineSeparator() +
                        "      --array-delimiter=<char>" + lineSeparator() +
                        "                             Delimiter character between array elements within" + lineSeparator() +
                        "                               a value in CSV data." + lineSeparator() +
                        "                               Default: ;" + lineSeparator() +
                        "      --quote=<char>         Character to treat as quotation character for" + lineSeparator() +
                        "                               values in CSV data. Quotes can be escaped as per" + lineSeparator() +
                        "                               RFC 4180 by doubling them, for example \"\" would" + lineSeparator() +
                        "                               be interpreted as a literal \". You cannot escape" + lineSeparator() +
                        "                               using \\." + lineSeparator() +
                        "                               Default: \"" + lineSeparator() +
                        "      --read-buffer-size=<size>" + lineSeparator() +
                        "                             Size of each buffer for reading input data. It has" + lineSeparator() +
                        "                               to at least be large enough to hold the biggest" + lineSeparator() +
                        "                               single value in the input data." + lineSeparator() +
                        "                               Default: 4194304" + lineSeparator() +
                        "      --max-memory=<size>    Maximum memory that neo4j-admin can use for" + lineSeparator() +
                        "                               various data structures and caching to improve" + lineSeparator() +
                        "                               performance. Values can be plain numbers, like" + lineSeparator() +
                        "                               10000000 or e.g. 20G for 20 gigabyte, or even e." + lineSeparator() +
                        "                               g. 70%." + lineSeparator() +
                        "                               Default: 90%" + lineSeparator() +
                        "      --high-io[=<true/false>]" + lineSeparator() +
                        "                             Ignore environment-based heuristics, and assume" + lineSeparator() +
                        "                               that the target storage subsystem can support" + lineSeparator() +
                        "                               parallel IO with high throughput." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --cache-on-heap[=<true/false>]" + lineSeparator() +
                        "                             (advanced) Whether or not to allow allocating" + lineSeparator() +
                        "                               memory for the cache on heap. If 'false' then" + lineSeparator() +
                        "                               caches will still be allocated off-heap, but the" + lineSeparator() +
                        "                               additional free memory inside the JVM will not" + lineSeparator() +
                        "                               be allocated for the caches. This to be able to" + lineSeparator() +
                        "                               have better control over the heap memory" + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --processors=<num>     (advanced) Max number of processors used by the" + lineSeparator() +
                        "                               importer. Defaults to the number of available" + lineSeparator() +
                        "                               processors reported by the JVM. There is a" + lineSeparator() +
                        "                               certain amount of minimum threads needed so for" + lineSeparator() +
                        "                               that reason there is no lower bound for this" + lineSeparator() +
                        "                               value. For optimal performance this value" + lineSeparator() +
                        "                               shouldn't be greater than the number of" + lineSeparator() +
                        "                               available processors." + lineSeparator() +
                        "                               Default: " + Runtime.getRuntime().availableProcessors() + lineSeparator() +
                        "      --bad-tolerance=<num>  Number of bad entries before the import is" + lineSeparator() +
                        "                               considered failed. This tolerance threshold is" + lineSeparator() +
                        "                               about relationships referring to missing nodes." + lineSeparator() +
                        "                               Format errors in input data are still treated as" + lineSeparator() +
                        "                               errors" + lineSeparator() +
                        "                               Default: 1000" + lineSeparator() +
                        "      --skip-bad-entries-logging[=<true/false>]" + lineSeparator() +
                        "                             Whether or not to skip logging bad entries" + lineSeparator() +
                        "                               detected during import." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --skip-bad-relationships[=<true/false>]" + lineSeparator() +
                        "                             Whether or not to skip importing relationships" + lineSeparator() +
                        "                               that refers to missing node ids, i.e. either" + lineSeparator() +
                        "                               start or end node id/group referring to node" + lineSeparator() +
                        "                               that wasn't specified by the node input data." + lineSeparator() +
                        "                               Skipped nodes will be logged, containing at most" + lineSeparator() +
                        "                               number of entities specified by bad-tolerance," + lineSeparator() +
                        "                               unless otherwise specified by" + lineSeparator() +
                        "                               skip-bad-entries-logging option." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --skip-duplicate-nodes[=<true/false>]" + lineSeparator() +
                        "                             Whether or not to skip importing nodes that have" + lineSeparator() +
                        "                               the same id/group. In the event of multiple" + lineSeparator() +
                        "                               nodes within the same group having the same id," + lineSeparator() +
                        "                               the first encountered will be imported whereas" + lineSeparator() +
                        "                               consecutive such nodes will be skipped. Skipped" + lineSeparator() +
                        "                               nodes will be logged, containing at most number" + lineSeparator() +
                        "                               of entities specified by bad-tolerance, unless" + lineSeparator() +
                        "                               otherwise specified by skip-bad-entries-logging" + lineSeparator() +
                        "                               option." + lineSeparator() +
                        "                               Default: false" + lineSeparator() +
                        "      --normalize-types[=<true/false>]" + lineSeparator() +
                        "                             Whether or not to normalize property types to" + lineSeparator() +
                        "                               Cypher types, e.g. 'int' becomes 'long' and" + lineSeparator() +
                        "                               'float' becomes 'double'" + lineSeparator() +
                        "                               Default: true" + lineSeparator() +
                        "      --nodes=[<label>[:<label>]...=]<files>..." + lineSeparator() +
                        "                             Node CSV header and data. Multiple files will be" + lineSeparator() +
                        "                               logically seen as one big file from the" + lineSeparator() +
                        "                               perspective of the importer. The first line must" + lineSeparator() +
                        "                               contain the header. Multiple data sources like" + lineSeparator() +
                        "                               these can be specified in one import, where each" + lineSeparator() +
                        "                               data source has its own header." + lineSeparator() +
                        "      --relationships=[<type>=]<files>..." + lineSeparator() +
                        "                             Relationship CSV header and data. Multiple files" + lineSeparator() +
                        "                               will be logically seen as one big file from the" + lineSeparator() +
                        "                               perspective of the importer. The first line must" + lineSeparator() +
                        "                               contain the header. Multiple data sources like" + lineSeparator() +
                        "                               these can be specified in one import, where each" + lineSeparator() +
                        "                               data source has its own header."
        );
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
        assertEquals( homeDir, resultingConfig.get( GraphDatabaseSettings.neo4j_home ).toFile() );
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
        assertEquals( homeDir, resultingConfig.get( GraphDatabaseSettings.neo4j_home ).toFile() );
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
            final var g = ImportCommand.parseNodeFilesGroup( "BANANA=" + testDir.absolutePath() + File.separator + "foo-[0-9].csv" );
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
            final var g = ImportCommand.parseRelationshipFilesGroup( "BANANA=" + testDir.absolutePath() + File.separator + "foo-[0-9].csv" );
            assertThat( g.key, equalTo( "BANANA" ) );
            assertThat( g.files, arrayContaining( foo1, foo2 ) );
        }
    }
}
