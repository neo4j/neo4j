/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.io.IOException;
import java.io.PrintWriter;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;

import static java.lang.String.format;

public class DbStructureTool
{
    private DbStructureTool()
    {
        throw new IllegalStateException( "Should not be instantiated" );
    }

    public static void main( String[] args ) throws IOException
    {
        if ( args.length != 2 && args.length != 3 )
        {
            System.err.println( "arguments: <generated class name> [<output source root>] <database dir>" );
            System.exit( 1 );
        }

        boolean writeToFile = args.length == 3;
        String generatedClassWithPackage = args[0];
        String dbDir = writeToFile ? args[2] : args[1];

        Pair<String, String> parsedGenerated = parseClassNameWithPackage( generatedClassWithPackage );
        String generatedClassPackage = parsedGenerated.first();
        String generatedClassName = parsedGenerated.other();

        String generator = format( "%s %s [<output source root>] <db-dir>",
                DbStructureTool.class.getCanonicalName(),
                generatedClassWithPackage
        );

        GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        try
        {
            if ( writeToFile )
            {
                File sourceRoot = new File( args[1] );
                String outputPackageDir = generatedClassPackage.replace( '.', File.separatorChar );
                String outputFileName = generatedClassName + ".java";
                File outputDir = new File( sourceRoot, outputPackageDir );
                File outputFile = new File( outputDir, outputFileName );
                try ( PrintWriter writer = new PrintWriter( outputFile ) )
                {
                    traceDb( generator, generatedClassPackage, generatedClassName, graph, writer );
                }
            }
            else
            {
                traceDb( generator, generatedClassPackage, generatedClassName, graph, System.out );
            }
        }
        finally
        {
            graph.shutdown();
        }
    }

    private static void traceDb( String generator,
                                 String generatedClazzPackage, String generatedClazzName,
                                 GraphDatabaseService graph,
                                 Appendable output )
            throws IOException
    {
        InvocationTracer<DbStructureVisitor> tracer = new InvocationTracer<>(
                generator,
                generatedClazzPackage,
                generatedClazzName,
                DbStructureVisitor.class,
                DbStructureArgumentFormatter.INSTANCE,
                output
        );

        DbStructureVisitor visitor = tracer.newProxy();
        GraphDbStructureGuide guide = new GraphDbStructureGuide( graph );
        guide.accept( visitor );
        tracer.close();
    }

    private static Pair<String, String> parseClassNameWithPackage( String classNameWithPackage )
    {
        if ( classNameWithPackage.contains( "%" ) )
        {
            throw new IllegalArgumentException(
                "Format character in generated class name: " + classNameWithPackage
            );
        }

        int index = classNameWithPackage.lastIndexOf( "." );

        if ( index < 0 )
        {
            throw new IllegalArgumentException(
                "Expected fully qualified class name but got: " + classNameWithPackage
            );
        }

        return Pair.of(
            classNameWithPackage.substring( 0, index ),
            classNameWithPackage.substring( index + 1 )
        );
    }
}
