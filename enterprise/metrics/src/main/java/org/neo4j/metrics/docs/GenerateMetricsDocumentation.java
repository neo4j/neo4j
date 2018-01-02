/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics.docs;

import java.io.File;
import java.util.List;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.FileUtils;

public class GenerateMetricsDocumentation
{
    private static final String OUTPUT_FILE_FLAG = "output";

    public static void main( String[] input ) throws Exception
    {
        Args args = Args.withFlags( OUTPUT_FILE_FLAG ).parse( input );

        List<String> metricsClassNames = args.orphans();
        if ( metricsClassNames.isEmpty() )
        {
            System.out.println( "Usage: GenerateMetricsDocumentation [--output file] className..." );
            System.exit( 1 );
        }

        MetricsAsciiDocGenerator generator = new MetricsAsciiDocGenerator();
        StringBuilder builder = new StringBuilder();
        for ( String className : metricsClassNames )
        {
            generator.generateDocsFor( className, builder );
        }

        String outputFileName = args.get( OUTPUT_FILE_FLAG );
        if ( outputFileName != null )
        {
            File output = new File( outputFileName );
            System.out.println( "Saving docs for '" + metricsClassNames + "' in '" + output.getAbsolutePath() + "'." );
            FileUtils.writeToFile( output, builder.toString(), false );
        }
        else
        {
            System.out.println( builder.toString() );
        }
    }
}
