/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.ha.upgrade;

import static java.lang.Runtime.getRuntime;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.copyURLToFile;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.io.IOUtils.copy;
import static org.neo4j.kernel.impl.util.FileUtils.deleteFile;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;
import static org.neo4j.kernel.impl.util.FileUtils.moveFile;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.TargetDirectory;

public class Utils
{
    private Utils()
    {
    }
    
    public static String assembleClassPathFromPackage( File directory )
    {
        List<File> jarFiles = new ArrayList<File>();
        FileFilter jarFilter = new FileFilter()
        {
            @Override
            public boolean accept( File file )
            {
                return file.isFile() && file.getName().endsWith( ".jar" );
            }
        };
        gatherFiles( jarFiles, directory, jarFilter );
        
        StringBuilder classpath = new StringBuilder();
        for ( File file : jarFiles )
            classpath.append( classpath.length() > 0 ? File.pathSeparator : "" ).append( file.getAbsolutePath() );
        return classpath.toString();
    }
    
    private static void gatherFiles( List<File> jarFiles, File directory, FileFilter filter )
    {
        for ( File file : directory.listFiles() )
        {
            if ( file.isDirectory() )
                gatherFiles( jarFiles, file, filter );
            else if ( filter.accept( file ) )
                jarFiles.add( file );
        }
    }

    public static File downloadAndUnpack( String url, File targetDirectory, String downloadedFileName ) throws IOException
    {
        URL website = new URL( url );
        File downloaded = new File( targetDirectory, downloadedFileName + ".zip" );
        if ( !downloaded.exists() )
        {
            File tmpDownload = new File( downloaded.getAbsolutePath() + ".tmp" );
            deleteFile( tmpDownload );
            copyURLToFile( website, tmpDownload );
            moveFile( tmpDownload, downloaded );
        }
        
        File unpacked = new File( targetDirectory, downloadedFileName );
        if ( !unpacked.exists() )
        {
            File tmpUnpack = new File( unpacked.getAbsolutePath() + "-tmp" );
            deleteRecursively( tmpUnpack );
            tmpUnpack.mkdirs();
            unzip( downloaded, tmpUnpack );
            FileUtils.moveFile( tmpUnpack, unpacked );
        }
        return unpacked;
    }
    
    public static List<File> unzip( File zipFile, File targetDir ) throws IOException
    {
        List<File> files = new ArrayList<File>();
        ZipFile zip = new ZipFile( zipFile );
        try
        {
            zip = new ZipFile( zipFile );
            for ( ZipEntry entry : Collections.list( zip.entries() ) )
            {
                File target = new File( targetDir, entry.getName() );
                target.getParentFile().mkdirs();
                if ( !entry.isDirectory() )
                {
                    InputStream input = zip.getInputStream( entry );
                    try
                    {
                        copyInputStreamToFile( input, target );
                        files.add( target );
                    }
                    finally
                    {
                        closeQuietly( input );
                    }
                }
            }
            return files;
        }
        finally
        {
            zip.close();
        }
    }
    
    public static void copyInputStreamToFile( InputStream stream, File target ) throws IOException
    {
        OutputStream out = null;
        try
        {
            out = new FileOutputStream( target );
            copy( stream, out );
        }
        finally
        {
            closeQuietly( out );
        }
    }
    
    public static Process execJava( String classPath, String mainClass, String... args ) throws Exception
    {
        List<String> allArgs = new ArrayList<String>( asList( "java", "-cp", classPath, mainClass ) );
        allArgs.addAll( asList( args ) );
        return getRuntime().exec( allArgs.toArray( new String[0] ) );
    }

    public static String zkConfig( TargetDirectory target, int id, int clientPort )
    {
        File config = target.file( "zookeeper" + id + ".cfg" );
        File dataDir = target.directory( "zk" + id + "data", true );
        try
        {
            PrintWriter conf = new PrintWriter( config );
            try
            {
                conf.println( "tickTime=2000" );
                conf.println( "initLimit=10" );
                conf.println( "syncLimit=5" );
                conf.println( "maxClientCnxns=50" );

                // On Windows the backslashes will have to be escaped for
                // ZooKeeper to interpret them correctly.
                conf.println( "dataDir=" + dataDir.getAbsolutePath().replaceAll( "\\\\", "\\\\\\\\" ) );

                conf.println( "clientPort=" + clientPort );
                for ( int j = 0; j < 1; j++ )
                {
                    conf.println( "server." + ( j + 1 ) + "=localhost:" + ( 2888 + j ) + ":"
                                  + ( 3888 + j ) );
                }
            }
            finally
            {
                conf.close();
            }
            PrintWriter myid = new PrintWriter( new File( dataDir, "myid" ) );
            try
            {
                myid.println( Integer.toString( id ) );
            }
            finally
            {
                myid.close();
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Could not write ZooKeeper configuration", e );
        }
        return config.getAbsolutePath();
    }
}
