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
package org.neo4j.kernel.impl.util.dbstructure;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class DbStructureTool
{
    private DbStructureTool()
    {
        throw new IllegalStateException( "Should not be instantiated" );
    }

    public static void main( String[] args ) throws FileNotFoundException
    {
        if ( args.length != 2 && args.length != 3 )
        {
            System.err.println( "arguments: <generated clazz name> [<output source root>] <database dir>" );
            System.exit( 1 );
        }

        boolean writeToFile = args.length == 3;
        String generatedClazz = args[0];
        String dbDir = writeToFile ? args[2] : args[1];

        GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        try
        {
            InvocationTracer<DbStructureVisitor> tracer = new InvocationTracer<>( generatedClazz, DbStructureVisitor.class );
            DbStructureVisitor visitor = tracer.newProxy();
            DbStructureGuide guide = new DbStructureGuide( graph );
            guide.accept( visitor );

            if ( writeToFile )
            {
                File sourceRoot = new File( args[1] );
                String outputPackageDir = tracer.getGeneratedPackageName().replace( '.', File.separatorChar );
                String outputFileName = tracer.getGeneratedClassName() + ".java";
                File outputDir = new File( sourceRoot, outputPackageDir );
                File outputFile = new File( outputDir, outputFileName );
                try ( PrintWriter writer = new PrintWriter( outputFile ) )
                {
                    writer.print( tracer.toString() );
                }
            }
            else
            {
                System.out.print( tracer.toString() );
            }
        }
        finally
        {
            graph.shutdown();
        }
    }
}
