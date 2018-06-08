/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.dbstructure;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.util.dbstructure.DbStructureArgumentFormatter;
import org.neo4j.kernel.impl.util.dbstructure.DbStructureVisitor;
import org.neo4j.kernel.impl.util.dbstructure.GraphDbStructureGuide;
import org.neo4j.kernel.impl.util.dbstructure.InvocationTracer;

import static java.lang.String.format;

public class DbStructureTool
{
    protected DbStructureTool()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        new DbStructureTool().run( args );
    }

    protected void run( String[] args ) throws IOException
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
                getClass().getCanonicalName(),
                generatedClassWithPackage
        );

        GraphDatabaseService graph = instantiateGraphDatabase( dbDir );
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

    protected GraphDatabaseService instantiateGraphDatabase( String dbDir )
    {
        return new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( new File( dbDir ) );
    }

    private void traceDb( String generator,
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

    private Pair<String, String> parseClassNameWithPackage( String classNameWithPackage )
    {
        if ( classNameWithPackage.contains( "%" ) )
        {
            throw new IllegalArgumentException(
                "Format character in generated class name: " + classNameWithPackage
            );
        }

        int index = classNameWithPackage.lastIndexOf( '.' );

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
