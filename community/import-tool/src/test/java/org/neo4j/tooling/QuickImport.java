/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.tooling;

import java.io.IOException;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;

import static org.neo4j.tooling.CsvDataGenerator.bareboneNodeHeader;
import static org.neo4j.tooling.CsvDataGenerator.bareboneRelationshipHeader;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.COMMAS;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.defaultVisible;

/**
 * Uses all available shortcuts to as quickly as possible import as much data as possible. Usage of this
 * utility is most likely just testing behavior of some components in the face of various dataset sizes,
 * even quite big ones. Uses the import tool, or rather directly the {@link ParallelBatchImporter}.
 *
 * Quick comes from gaming terminology where you sometimes just want to play a quick game, without
 * any settings or hazzle, just play.
 *
 * Uses {@link CsvDataGeneratorInput} as random data {@link Input}.
 *
 * For the time being the node/relationship data can't be controlled via command-line arguments,
 * only through changing the code. The {@link CsvDataGeneratorInput} accepts two {@link Header headers}
 * describing which sort of data it should generate.
 */
public class QuickImport
{
    public static void main( String[] arguments ) throws IOException
    {
        Args args = Args.parse( arguments );
        long nodeCount = args.getNumber( "nodes", null ).longValue();
        long relationshipCount = args.getNumber( "relationships", null ).longValue();
        int labelCount = args.getNumber( "labels", 4 ).intValue();
        int relationshipTypeCount = args.getNumber( "relationship-types", 4 ).intValue();
        String dir = args.get( ImportTool.Options.STORE_DIR.key() );

        Extractors extractors = new Extractors( COMMAS.arrayDelimiter() );
        IdType idType = IdType.ACTUAL;
        Input input = new CsvDataGeneratorInput(
                bareboneNodeHeader( idType, extractors ), bareboneRelationshipHeader( idType, extractors ),
                COMMAS, nodeCount, relationshipCount, new Groups(), idType, labelCount, relationshipTypeCount );
        BatchImporter importer = new ParallelBatchImporter( dir, DEFAULT, new SystemOutLogging(), defaultVisible() );
        importer.doImport( input );
    }
}
