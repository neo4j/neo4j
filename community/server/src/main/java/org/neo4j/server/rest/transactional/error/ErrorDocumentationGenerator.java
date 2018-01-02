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
package org.neo4j.server.rest.transactional.error;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * Generates Asciidoc for {@link Status}.
 *
 * <pre>
 * [options="header", cols="&gt;s,^", width="100%"]
 * |===
 * Status Code                                    |Description
 * Neo.SomeClassification.SomeCategory.SomeTitle  |Some description
 * |===
 * </pre>
 */
public class ErrorDocumentationGenerator
{
    public static void main( String[] args ) throws Exception
    {
        File baseDir = getBaseDirectory( args );

        ErrorDocumentationGenerator generator = new ErrorDocumentationGenerator();

        try
        {
            generateDocumentation(
                    generator.generateClassificationDocs(),
                    new File( baseDir, "status-code-classifications.asccidoc" ),
                    "status code classification" );

            generateDocumentation(
                    generator.generateStatusCodeDocs(),
                    new File( baseDir, "status-code-codes.asccidoc" ),
                    "status code statuses");
        }
        catch ( Exception e )
        {
            // Send it to standard out, so we can see it through the Maven build.
            e.printStackTrace( System.out );
            System.out.flush();
            System.exit( 1 );
        }
    }

    private static File getBaseDirectory( String[] args )
    {
        File baseDir = null;
        if(args.length == 1)
        {
            baseDir = new File(args[0]);

        } else
        {
            System.out.println("Usage: ErrorDocumentationGenerator [output folder]");
            System.exit(0);
        }
        return baseDir;
    }

    private static void generateDocumentation( Table table, File file, String description ) throws Exception
    {
        System.out.printf( "Saving %s docs in '%s'.%n", description, file.getAbsolutePath() );
        file.getParentFile().mkdirs();
        try ( PrintStream out = new PrintStream( file, "UTF-8" ) )
        {
            table.print( out );
        }
    }

    public Table generateClassificationDocs()
    {
        Table table = new Table();
        table.setCols( "<1m,<3,<1" );
        table.setHeader( "Classification", "Description", "Effect on transaction" );

        for ( Status.Classification classification : Status.Classification.class.getEnumConstants() )
        {
            table.addRow( classificationAsRow( classification ) );
        }

        return table;
    }

    private Object[] classificationAsRow( Status.Classification classification )
    {
        // TODO fail on missing description
        String description = classification.description().length() > 0
                ? classification.description()
                : "No description available.";
        String txEffect = classification.rollbackTransaction() ? "Rollback" : "None";
        return new Object[] { classification.name(), description, txEffect };
    }

    public Table generateStatusCodeDocs()
    {
        Table table = new Table();
        table.setCols( "<1m,<1" );
        table.setHeader( "Status Code", "Description" );

        TreeMap<String, Status.Code> sortedStatuses = sortedStatusCodes();
        for ( String code : sortedStatuses.keySet() )
        {
            Status.Code statusCode = sortedStatuses.get( code );
            table.addRow( codeAsTableRow( statusCode ) );
        }

        return table;
    }

    private Object[] codeAsTableRow( Status.Code code )
    {
        // TODO fail on missing description
        String description = code.description().length() > 0 ? code.description() : "No description available.";
        return new Object[] { code.serialize(), description };
    }

    private TreeMap<String, Status.Code> sortedStatusCodes()
    {
        TreeMap<String, Status.Code> sortedStatuses = new TreeMap<>();
        for ( Status status : Status.Code.all() )
        {
            sortedStatuses.put( status.code().serialize(), status.code() );
        }
        return sortedStatuses;
    }


    public static class Table
    {
        private String cols;
        private String[] header;
        private List<Object[]> rows = new ArrayList<>();

        public void setCols( String cols )
        {
            this.cols = cols;
        }

        public void setHeader( String... header )
        {
            this.header = header;
        }

        public void addRow( Object... xs )
        {
            rows.add(xs);
        }

        public void print( PrintStream out )
        {
            out.printf( "[options=\"header\", cols=\"%s\"]%n", cols );
            out.printf( "|===%n" );

            for( String columnHeader : header )
            {
                out.printf( "|%s ", columnHeader );
            }
            out.printf( "%n" );

            for( Object[] row : rows )
            {
                for( Object cell : row )
                {
                    out.printf( "|%s ", cell );
                }
                out.printf( "%n" );
            }

            out.printf( "|===%n" );
        }
    }
}
