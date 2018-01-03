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
package org.neo4j.doc.cypherdoc;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;

/**
 * Parses AsciiDoc files with some special markup to produce Cypher tutorials.
 */
public class Main
{
    private static final String[] EXTENSIONS = new String[] { ".asciidoc", ".adoc" };
    private static final IOFileFilter fileFilter = new SuffixFileFilter( EXTENSIONS );

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
        if ( args.length < 3 )
        {
            throw new IllegalArgumentException(
                    "Destination directory, public URL and at least one source must be specified." );
        }

        File destinationDir = getDestinationDir( args[0] );
        String destinationUrl = args[1];

        for ( int i = 2; i < args.length; i++ )
        {
            String name = args[i];
            File source = FileUtils.getFile( name ).getCanonicalFile();
            if ( source.isFile() )
            {
                executeFile( source, destinationDir, destinationUrl );
            }
            else if ( source.isDirectory() )
            {
                executeDirectory( source, destinationDir, destinationUrl, true );
            }
        }
    }

    private static void executeDirectory( File sourceDir, File destinationDir, String destinationUrl, boolean isTopLevelDir )
    {
        String sourceDirName = sourceDir.getName();
        File nestedDestinationDir = isTopLevelDir ? destinationDir : new File( destinationDir, sourceDirName );
        String nestedDestinationUrl = isTopLevelDir ? destinationUrl : destinationUrl + '/' + sourceDirName;
        File[] files = sourceDir.listFiles( new FileFilter()
        {
            @Override
            public boolean accept( File file )
            {
                return file.isDirectory() || fileFilter.accept( file );
            }
        } );
        for ( File fileInDir : files )
        {
            if ( fileInDir.isDirectory() )
            {
                executeDirectory( fileInDir, nestedDestinationDir, nestedDestinationUrl, false );
            }
            else
            {
                try
                {
                    executeFile( fileInDir, nestedDestinationDir, nestedDestinationUrl );
                }
                catch ( Throwable e )
                {
                    throw new RuntimeException( String.format( "Failed while executing file: %s in the "
                                                               + "directory %s", fileInDir.getName(),
                            destinationDir.getAbsolutePath() ), e );
                }
            }
        }
    }

    private static File getDestinationDir( String arg ) throws IOException
    {
        String name = arg;
        File file = FileUtils.getFile( name );
        if ( file.exists() && !file.isDirectory() )
        {
            throw new IllegalArgumentException(
                    "Destination directory must either not exist or be a directory." );
        }
        return file.getCanonicalFile();
    }

    /**
     * Parse a single file.
     */
    private static void executeFile( File sourceFile, File destinationDir, String url ) throws Exception
    {
        try
        {
            String name = sourceFile.getName();
            String input = FileUtils.readFileToString( sourceFile, StandardCharsets.UTF_8 );
            String output = CypherDoc.parse( input, sourceFile.getParentFile(), url );

            FileUtils.forceMkdir( destinationDir );
            File targetFile = FileUtils.getFile( destinationDir, name );
            FileUtils.writeStringToFile( targetFile, output, StandardCharsets.UTF_8 );
        }
        catch ( TestFailureException failure )
        {
            failure.dumpSnapshots( destinationDir );
            throw failure;
        }
    }
}
