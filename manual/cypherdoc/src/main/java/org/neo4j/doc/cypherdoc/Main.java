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
package org.neo4j.doc.cypherdoc;

import java.io.File;

import org.apache.commons.io.FileUtils;

/**
 * Parses AsciiDoc files with some special markup to produce Cypher tutorials.
 */
public class Main
{
    private static final String[] EXTENSIONS = new String[] { "asciidoc", "adoc" };

    /**
     * Transforms the given files or directories (searched recursively for
     * .asciidoc or .adoc files). The output file name is based on the input
     * file name (and the relative path if a directory got searched). The first
     * argument is the base destination directory.
     * 
     * @param args base destination directory, followed by files/directories to parse.
     */
    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 2 )
        {
            throw new IllegalArgumentException(
                    "Destination directory and at least one source must be specified." );
        }
        File destination = null;
        for ( String name : args )
        {
            File file = FileUtils.getFile( name );
            if ( destination == null )
            {
                if ( file.exists() && !file.isDirectory() )
                {
                    throw new IllegalArgumentException(
                            "Destination directory must either not exist or be a directory." );
                }
                destination = file;
            }
            else
            {
                if ( file.isFile() )
                {
                    executeFile( file, file.getName(), destination );
                }
                else if ( file.isDirectory() )
                {
                    for ( File fileInDir : FileUtils.listFiles( file,
                            EXTENSIONS, true ) )
                    {
                        String fileInDirName = fileInDir.getAbsolutePath()
                                .substring( file.getAbsolutePath().length() + 1 )
                                .replace( '/', '-' )
                                .replace( '\\', '-' );
                        executeFile( fileInDir, fileInDirName, destination );
                    }
                }
            }
        }
    }

    /**
     * Parse a single file.
     */
    private static void executeFile( File file, String name, File destinationDir ) throws Exception
    {
        try
        {
            String input = FileUtils.readFileToString( file );
            String output = CypherDoc.parse( input );

            FileUtils.forceMkdir( destinationDir );
            File targetFile = FileUtils.getFile( destinationDir, name );
            FileUtils.writeStringToFile( targetFile, output );
        }
        catch ( TestFailureException failure )
        {
            failure.dumpSnapshots( destinationDir );
            throw failure;
        }
    }
}
