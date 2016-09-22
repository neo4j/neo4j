/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.dbms;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collection;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.tooling.ImportTool;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.csv.CsvInput;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;

import static java.lang.Math.toIntExact;
import static java.nio.charset.Charset.defaultCharset;
import static org.neo4j.kernel.configuration.Settings.parseLongWithUnit;
import static org.neo4j.kernel.impl.util.Converters.withDefault;
import static org.neo4j.tooling.ImportTool.csvConfiguration;
import static org.neo4j.tooling.ImportTool.extractInputFiles;
import static org.neo4j.tooling.ImportTool.importConfiguration;
import static org.neo4j.tooling.ImportTool.nodeData;
import static org.neo4j.tooling.ImportTool.relationshipData;
import static org.neo4j.tooling.ImportTool.validateInputFiles;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.badCollector;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.collect;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;

class CsvImporter implements Importer
{
    private final Collection<Args.Option<File[]>> nodesFiles, relationshipsFiles;
    private final IdType idType;
    private final Charset inputEncoding;
    private final int pageSize;
    private final Config config;
    private final Args args;
    private final OutsideWorld outsideWorld;

    public static String description()
    {
        return "--mode=csv Import a database from a collection of CSV files.\n" +
                "--nodes[:Label1:Label2]=\"<file1>,<file2>,...\"\n" +
                "        Node CSV header and data. Multiple files will be logically seen as\n" +
                "        one big file from the perspective of the importer. The first line\n" +
                "        must contain the header. Multiple data sources like these can be\n" +
                "        specified in one import, where each data source has its own header.\n" +
                "        Note that file groups must be enclosed in quotation marks.\n" +
                "--relationships[:RELATIONSHIP_TYPE] \"<file1>,<file2>,...\"\n" +
                "        Relationship CSV header and data. Multiple files will be logically\n" +
                "        seen as one big file from the perspective of the importer. The first\n" +
                "        line must contain the header. Multiple data sources like these can be\n" +
                "        specified in one import, where each data source has its own header.\n" +
                "        Note that file groups must be enclosed in quotation marks.\n" + "--id-type <id-type>\n" +
                "        Each node must provide a unique id. This is used to find the correct\n" +
                "        nodes when creating relationships. Must be one of:\n" +
                "            STRING: (default) arbitrary strings for identifying nodes.\n" +
                "            INTEGER: arbitrary integer values for identifying nodes.\n" +
                "            ACTUAL: (advanced) actual node ids. The default option is STRING.\n" +
                "        For more information on id handling, please see the Neo4j Manual:\n" +
                "        http://neo4j.com/docs/operations-manual/current/deployment/#import-tool\n" +
                "--input-encoding <character-set>\n" +
                "        Character set that input data is encoded in. Defaults to UTF-8.\n" +
                "--page-size <page-size>\n" + "        Page size to use for import in bytes. (e. g. 4M or 8k)\n";
    }

    public static String arguments()
    {
        return "[--nodes[:Label1:Label2]=\"<file1>,<file2>,...\"] " +
                "[--relationships[:RELATIONSHIP_TYPE]=\"<file1>,<file2>,...\"] " +
                "[--input-encoding=<character-set>] " + "[--id-type=<id-type>] ";
    }

    CsvImporter( Args args, Config config, OutsideWorld outsideWorld ) throws IncorrectUsage
    {
        this.args = args;
        this.outsideWorld = outsideWorld;
        nodesFiles = extractInputFiles( args, "nodes", outsideWorld.errorStream() );
        relationshipsFiles = extractInputFiles( args, "relationships", outsideWorld.errorStream() );
        try
        {
            validateInputFiles( nodesFiles, relationshipsFiles );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        idType = args.interpretOption( "id-type", withDefault( IdType.STRING ),
                from -> IdType.valueOf( from.toUpperCase() ) );
        inputEncoding = Charset.forName( args.get( "input-encoding", defaultCharset().name() ) );
        pageSize = toIntExact(
                parseLongWithUnit( args.get( "page-size", String.valueOf( Configuration.DEFAULT.pageSize() ) ) ) );
        this.config = config;
    }

    @Override
    public void doImport() throws IOException
    {
        OutputStream badOutput = outsideWorld.errorStream();
        Collector badCollector = badCollector( badOutput, 1000, collect( true, false, false ) );

        Configuration configuration = importConfiguration( null, false, config, pageSize );
        CsvInput input = new CsvInput( nodeData( inputEncoding, nodesFiles ), defaultFormatNodeFileHeader(),
                relationshipData( inputEncoding, relationshipsFiles ), defaultFormatRelationshipFileHeader(), idType,
                csvConfiguration( args, false ), badCollector, configuration.maxNumberOfProcessors() );

        ImportTool.doImport( outsideWorld.errorStream(), outsideWorld.errorStream(),
                config.get( DatabaseManagementSystemSettings.database_path ), nodesFiles, relationshipsFiles, false,
                input, config, badOutput, configuration );
    }
}
