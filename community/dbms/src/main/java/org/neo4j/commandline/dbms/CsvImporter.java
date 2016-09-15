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

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.tooling.ImportTool;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.database_path;

class CsvImporter implements Importer
{
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
                "        Note that file groups must be enclosed in quotation marks.\n" +
                "--id-type <id-type>\n" +
                "        Each node must provide a unique id. This is used to find the correct\n" +
                "        nodes when creating relationships. Must be one of:\n" +
                "            STRING: (default) arbitrary strings for identifying nodes.\n" +
                "            INTEGER: arbitrary integer values for identifying nodes.\n" +
                "            ACTUAL: (advanced) actual node ids. The default option is STRING.\n" +
                "        For more information on id handling, please see the Neo4j Manual:\n" +
                "        http://neo4j.com/docs/operations-manual/current/deployment/#import-tool\n" +
                "--input-encoding <character-set>\n" +
                "        Character set that input data is encoded in. Defaults to UTF-8.\n" +
                "--page-size <page-size>\n" +
                "        Page size to use for import in bytes. (e. g. 4M or 8k)\n";
    }

    public static String arguments()
    {
        return "[--nodes[:Label1:Label2]=\"<file1>,<file2>,...\"] " +
                "[--relationships[:RELATIONSHIP_TYPE]=\"<file1>,<file2>,...\"] " +
                "[--input-encoding=<character-set>] " +
                "[--id-type=<id-type>] ";
    }

    private String[] args;

    CsvImporter( String[] args, Config config )
    {
        this.args = Arrays.copyOf( args, args.length + 1 );
        this.args[args.length] = String.format( "--into=%s", config.get( database_path ) );
    }

    @Override
    public void doImport() throws IOException
    {
        ImportTool.main( args );
    }
}
