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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Optional;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class ImportCommandTest
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void shouldUseArgumentsFoundInside_f_Argument() throws FileNotFoundException, CommandFailed, IncorrectUsage
    {
        // given
        File report = testDir.file( "report" );
        ImportCommand importCommand =
                new ImportCommand( testDir.directory( "home" ).toPath(), testDir.directory( "conf" ).toPath(),
                        new RealOutsideWorld( System.out, System.err, new ByteArrayInputStream( new byte[0] ) ) );
        File nodesFile = createTextFile( "nodes.csv", ":ID", "1", "2" );
        String pathWithEscapedSpaces = escapeSpaces( nodesFile.getAbsolutePath() );
        String reportEscapedPath = escapeSpaces( report.getAbsolutePath() );
        File argFile = createTextFile( "args.txt", "--database=foo", "--nodes=" + pathWithEscapedSpaces, "--report-file=" + reportEscapedPath );
        String[] arguments = {"-f", argFile.getAbsolutePath()};

        // when
        importCommand.execute( arguments );

        // then
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "IMPORT DONE" ) );
        assertTrue( suppressOutput.getOutputVoice().containsMessage( nodesFile.getAbsolutePath() ) );
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "2 nodes" ) );
    }

    @Test
    void shouldPrintNiceHelp() throws Throwable
    {
        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
            usage.printUsageForCommand( new ImportCommandProvider(), ps::println );

            assertEquals( String.format( "usage: neo4j-admin import [--database=<name>]%n" +
                            "                          [--additional-config=<config-file-path>]%n" +
                            "                          [--report-file=<filename>]%n" +
                            "                          [--nodes[:Label1:Label2]=<\"file1,file2,...\">]%n" +
                            "                          [--relationships[:RELATIONSHIP_TYPE]=<\"file1,file2,...\">]%n" +
                            "                          [--id-type=<STRING|INTEGER|ACTUAL>]%n" +
                            "                          [--input-encoding=<character-set>]%n" +
                            "                          [--ignore-extra-columns[=<true|false>]]%n" +
                            "                          [--multiline-fields[=<true|false>]]%n" +
                            "                          [--delimiter=<delimiter-character>]%n" +
                            "                          [--array-delimiter=<array-delimiter-character>]%n" +
                            "                          [--quote=<quotation-character>]%n" +
                            "                          [--max-memory=<max-memory-that-importer-can-use>]%n" +
                            "                          [--f=<File containing all arguments to this import>]%n" +
                            "                          [--high-io[=<true|false>]]%n" +
                            "                          [--bad-tolerance=<max-number-of-bad-entries-or-'-1'-for-unlimited>]%n" +
                            "                          [--cache-on-heap[=<true|false>]]%n" +
                            "                          [--detailed-progress[=<true|false>]]%n" +
                            "                          [--ignore-empty-strings[=<true|false>]]%n" +
                            "                          [--legacy-style-quoting[=<true|false>]]%n" +
                            "                          [--read-buffer-size=<bytes, e.g. 10k, 4M>]%n" +
                            "                          [--skip-bad-entries-logging[=<true|false>]]%n" +
                            "                          [--skip-bad-relationships[=<true|false>]]%n" +
                            "                          [--skip-duplicate-nodes[=<true|false>]]%n" +
                            "                          [--processors=<max processor count>]%n" +
                            "                          [--trim-strings[=<true|false>]]%n" +
                            "                          [--normalize-types[=<true|false>]]%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "Import a collection of CSV files.%n" +
                            "%n" +
                            "options:%n" +
                            "  --database=<name>%n" +
                            "      Name of database. [default:" + GraphDatabaseSettings.DEFAULT_DATABASE_NAME + "]%n" +
                            "  --additional-config=<config-file-path>%n" +
                            "      Configuration file to supply additional configuration in. [default:]%n" +
                            "  --report-file=<filename>%n" +
                            "      File in which to store the report of the csv-import.%n" +
                            "      [default:import.report]%n" +
                            "  --nodes[:Label1:Label2]=<\"file1,file2,...\">%n" +
                            "      Node CSV header and data. Multiple files will be logically seen as one big%n" +
                            "      file from the perspective of the importer. The first line must contain the%n" +
                            "      header. Multiple data sources like these can be specified in one import,%n" +
                            "      where each data source has its own header. Note that file groups must be%n" +
                            "      enclosed in quotation marks. [default:]%n" +
                            "  --relationships[:RELATIONSHIP_TYPE]=<\"file1,file2,...\">%n" +
                            "      Relationship CSV header and data. Multiple files will be logically seen as%n" +
                            "      one big file from the perspective of the importer. The first line must%n" +
                            "      contain the header. Multiple data sources like these can be specified in%n" +
                            "      one import, where each data source has its own header. Note that file%n" +
                            "      groups must be enclosed in quotation marks. [default:]%n" +
                            "  --id-type=<STRING|INTEGER|ACTUAL>%n" +
                            "      Each node must provide a unique id. This is used to find the correct nodes%n" +
                            "      when creating relationships. Possible values are:%n" +
                            "        STRING: arbitrary strings for identifying nodes,%n" +
                            "        INTEGER: arbitrary integer values for identifying nodes,%n" +
                            "        ACTUAL: (advanced) actual node ids.%n" +
                            "      For more information on id handling, please see the Neo4j Manual:%n" +
                            "      https://neo4j.com/docs/operations-manual/current/tools/import/%n" +
                            "      [default:STRING]%n" +
                            "  --input-encoding=<character-set>%n" +
                            "      Character set that input data is encoded in. [default:UTF-8]%n" +
                            "  --ignore-extra-columns=<true|false>%n" +
                            "      If un-specified columns should be ignored during the import.%n" +
                            "      [default:false]%n" +
                            "  --multiline-fields=<true|false>%n" +
                            "      Whether or not fields from input source can span multiple lines, i.e.%n" +
                            "      contain newline characters. [default:false]%n" +
                            "  --delimiter=<delimiter-character>%n" +
                            "      Delimiter character between values in CSV data. [default:,]%n" +
                            "  --array-delimiter=<array-delimiter-character>%n" +
                            "      Delimiter character between array elements within a value in CSV data.%n" +
                            "      [default:;]%n" +
                            "  --quote=<quotation-character>%n" +
                            "      Character to treat as quotation character for values in CSV data. Quotes%n" +
                            "      can be escaped as per RFC 4180 by doubling them, for example \"\" would be%n" +
                            "      interpreted as a literal \". You cannot escape using \\. [default:\"]%n" +
                            "  --max-memory=<max-memory-that-importer-can-use>%n" +
                            "      Maximum memory that neo4j-admin can use for various data structures and%n" +
                            "      caching to improve performance. Values can be plain numbers, like 10000000%n" +
                            "      or e.g. 20G for 20 gigabyte, or even e.g. 70%%. [default:90%%]%n" +
                            "  --f=<File containing all arguments to this import>%n" +
                            "      File containing all arguments, used as an alternative to supplying all%n" +
                            "      arguments on the command line directly.Each argument can be on a separate%n" +
                            "      line or multiple arguments per line separated by space.Arguments%n" +
                            "      containing spaces needs to be quoted.Supplying other arguments in addition%n" +
                            "      to this file argument is not supported. [default:]%n" +
                            "  --high-io=<true|false>%n" +
                            "      Ignore environment-based heuristics, and assume that the target storage%n" +
                            "      subsystem can support parallel IO with high throughput. [default:true]%n" +
                            "  --bad-tolerance=<max-number-of-bad-entries-or-'-1'-for-unlimited>%n" +
                            "      Number of bad entries before the import is considered failed. This%n" +
                            "      tolerance threshold is about relationships referring to missing nodes.%n" +
                            "      Format errors in input data are still treated as errors [default:1000]%n" +
                            "  --cache-on-heap=<true|false>%n" +
                            "      (advanced) Whether or not to allow allocating memory for the cache on%n" +
                            "      heap. If 'false' then caches will still be allocated off-heap, but the%n" +
                            "      additional free memory inside the JVM will not be allocated for the%n" +
                            "      caches. This to be able to have better control over the heap memory%n" +
                            "      [default:false]%n" +
                            "  --detailed-progress=<true|false>%n" +
                            "      Use the old detailed 'spectrum' progress printing [default:false]%n" +
                            "  --ignore-empty-strings=<true|false>%n" +
                            "      Whether or not empty string fields, i.e. \"\" from input source are ignored,%n" +
                            "      i.e. treated as null. [default:false]%n" +
                            "  --legacy-style-quoting=<true|false>%n" +
                            "      Whether or not backslash-escaped quote e.g. \\\" is interpreted as inner%n" +
                            "      quote. [default:false]%n" +
                            "  --read-buffer-size=<bytes, e.g. 10k, 4M>%n" +
                            "      Size of each buffer for reading input data. It has to at least be large%n" +
                            "      enough to hold the biggest single value in the input data.%n" +
                            "      [default:4194304]%n" +
                            "  --skip-bad-entries-logging=<true|false>%n" +
                            "      Whether or not to skip logging bad entries detected during import.%n" +
                            "      [default:false]%n" +
                            "  --skip-bad-relationships=<true|false>%n" +
                            "      Whether or not to skip importing relationships that refers to missing node%n" +
                            "      ids, i.e. either start or end node id/group referring to node that wasn't%n" +
                            "      specified by the node input data. Skipped nodes will be logged, containing%n" +
                            "      at most number of entities specified by bad-tolerance, unless otherwise%n" +
                            "      specified by skip-bad-entries-logging option. [default:false]%n" +
                            "  --skip-duplicate-nodes=<true|false>%n" +
                            "      Whether or not to skip importing nodes that have the same id/group. In the%n" +
                            "      event of multiple nodes within the same group having the same id, the%n" +
                            "      first encountered will be imported whereas consecutive such nodes will be%n" +
                            "      skipped. Skipped nodes will be logged, containing at most number of%n" +
                            "      entities specified by bad-tolerance, unless otherwise specified by%n" +
                            "      skip-bad-entries-logging option. [default:false]%n" +
                            "  --processors=<max processor count>%n" +
                            "      (advanced) Max number of processors used by the importer. Defaults to the%n" +
                            "      number of available processors reported by the JVM%n" +
                            "      skip-bad-entries-logging. There is a certain amount of minimum threads%n" +
                            "      needed so for that reason there is no lower bound for this value. For%n" +
                            "      optimal performance this value shouldn't be greater than the number of%n" +
                            "      available processors. [default:null]%n" +
                            "  --trim-strings=<true|false>%n" +
                            "      Whether or not strings should be trimmed for whitespaces. [default:true]%n" +
                            "  --normalize-types=<true|false>%n" +
                            "      Whether or not to normalize property types to Cypher types, e.g. 'int'%n" +
                            "      becomes 'long' and 'float' becomes 'double' [default:true]%n" ),
                    baos.toString() );
        }
    }

    @Test
    void shouldKeepSpecifiedNeo4jHomeWhenAdditionalConfigIsPresent() throws CommandFailed, IncorrectUsage, IOException
    {
        // given
        File homeDir = testDir.directory( "other", "place" );
        File additionalConfigFile = testDir.createFile( "empty.conf" );
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld( System.out, System.err, new ByteArrayInputStream( new byte[0] ) ) )
        {
            ImportCommand command = new ImportCommand( homeDir.toPath(), testDir.directory( "conf" ).toPath(), outsideWorld );

            // when
            Config resultingConfig = command.loadNeo4jConfig( Optional.of( additionalConfigFile.toPath() ) );

            // then
            assertEquals( homeDir, resultingConfig.get( GraphDatabaseSettings.neo4j_home ) );
        }
    }

    @Test
    void shouldKeepSpecifiedNeo4jHomeWhenNoAdditionalConfigIsPresent() throws CommandFailed, IncorrectUsage, IOException
    {
        // given
        File homeDir = testDir.directory( "other", "place" );
        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld( System.out, System.err, new ByteArrayInputStream( new byte[0] ) ) )
        {
            ImportCommand command = new ImportCommand( homeDir.toPath(), testDir.directory( "conf" ).toPath(), outsideWorld );

            // when
            Config resultingConfig = command.loadNeo4jConfig( Optional.empty() );

            // then
            assertEquals( homeDir, resultingConfig.get( GraphDatabaseSettings.neo4j_home ) );
        }
    }

    private File createTextFile( String name, String... lines ) throws FileNotFoundException
    {
        File file = testDir.file( name );
        try ( PrintStream out = new PrintStream( file ) )
        {
            for ( String line : lines )
            {
                out.println( line );
            }
        }
        return file;
    }

    private static String escapeSpaces( String pathForFile )
    {
        return pathForFile.replaceAll( " ", "\\\\ " );
    }
}
