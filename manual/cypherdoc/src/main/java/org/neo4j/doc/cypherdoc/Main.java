/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.doc.cypherdoc;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

/**
 * Parses AsciiDoc files with some special markup to produce Cypher tutorials.
 */
public class Main
{
    private static final String[] EXTENSIONS = new String[] { "asciidoc",
            "adoc" };

    /**
     * Transforms the given files or directories (searched recursively for
     * .asciidoc or .adoc files). The result is found in the
     * target/docs/cypherdoc directory. The output file name is based on the
     * input file name (and the relative path if a directory got searched).
     * 
     * @param args files/directories to parse.
     */
    public static void main( String[] args )
    {
        for ( String name : args )
        {
            File file = FileUtils.getFile( name );
            if ( file.isFile() )
            {
                executeFile( file, file.getName() );
            }
            else if ( file.isDirectory() )
            {
                for ( File fileInDir : FileUtils.listFiles( file, EXTENSIONS,
                        true ) )
                {
                    String fileInDirName = fileInDir.getAbsolutePath()
                            .substring( (int) file.getAbsolutePath()
                                    .length() + 1 )
                            .replace( '/', '-' )
                            .replace( '\\', '-' );
                    executeFile( fileInDir, fileInDirName );
                }
            }
        }
    }

    /**
     * Parse a single file.
     */
    private static void executeFile( File file, String name )
    {
        try
        {
            String input = FileUtils.readFileToString( file );
            String output = CypherDoc.parse( input );

            File targetDir = FileUtils.getFile( "target", "docs", "cypherdoc" );
            FileUtils.forceMkdir( targetDir );
            File targetFile = FileUtils.getFile( targetDir, name );
            FileUtils.writeStringToFile( targetFile, output );
        }
        catch ( IOException ioe )
        {
            ioe.printStackTrace();
        }
    }
}
