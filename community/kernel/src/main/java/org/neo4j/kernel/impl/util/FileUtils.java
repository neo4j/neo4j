/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Pattern;

import org.neo4j.graphdb.NotFoundException;

public class FileUtils
{
    private static int WINDOWS_RETRY_COUNT = 3;

    public static void deleteRecursively( File directory )
    throws IOException
    {
        Stack<File> stack = new Stack<File>();
        List<File> temp = new LinkedList<File>();
        stack.push(directory.getAbsoluteFile());
        while(!stack.isEmpty())
        {
            File top = stack.pop();
            if (top.listFiles() != null)
            {
                for (File child : top.listFiles()) {
                    if (child.isFile()) {
                        if ( !deleteFile( child ) )
                        {
                            throw new IOException( "Failed to delete "
                                    + child.getCanonicalPath() );
                        }
                    } else {
                        temp.add(child);
                    }
                }
            }
            if (top.listFiles() == null || top.listFiles().length == 0) {
                if ( !deleteFile( top ) )
                {
                    throw new IOException( "Failed to delete "
                            + top.getCanonicalPath() );
                }
            } else {
                stack.push(top);
                for (File f : temp)
                {
                    stack.push(f);
                }
            }
            temp.clear();
        }
    }

    public static boolean deleteFile( File file )
    {
        if ( !file.exists() )
        {
            return true;
        }
        int count = 0;
        boolean deleted = false;
        do
        {
            deleted = file.delete();
            if ( !deleted )
            {
                count++;
                waitSome();
            }
        }
        while ( !deleted && count <= WINDOWS_RETRY_COUNT );
        return deleted;
    }
    
    public static File[] deleteFiles( File directory, String regexPattern )
            throws IOException
    {
        Pattern pattern = Pattern.compile( regexPattern );
        Collection<File> deletedFiles = new ArrayList<File>();
        for ( File file : directory.listFiles() )
        {
            if ( pattern.matcher( file.getName() ).find() )
            {
                if ( !file.delete() )
                {
                    throw new IOException( "Couldn't delete file '" + file.getAbsolutePath() + "'" );
                }
                deletedFiles.add( file );
            }
        }
        return deletedFiles.toArray( new File[deletedFiles.size()] );
    }

    public static boolean renameFile( File srcFile, File renameToFile )
    {
        if ( !srcFile.exists() )
        {
            throw new NotFoundException( "Source file[" + srcFile.getName()
                    + "] not found" );
        }
        if ( renameToFile.exists() )
        {
            throw new NotFoundException( "Target file[" + renameToFile.getName()
                    + "] already exists" );
        }
        int count = 0;
        boolean renamed = false;
        do
        {
            renamed = srcFile.renameTo( renameToFile );
            if ( !renamed )
            {
                count++;
                waitSome();
            }
        }
        while ( !renamed && count <= WINDOWS_RETRY_COUNT );
        return renamed;
    }

    public static void truncateFile( FileChannel fileChannel, long position )
    throws IOException
    {
        int count = 0;
        boolean success = false;
        IOException cause = null;
        do
        {
            count++;
            try
            {
                fileChannel.truncate( position );
                success = true;
            }
            catch ( IOException e )
            {
                cause = e;
            }

        }
        while ( !success && count <= WINDOWS_RETRY_COUNT );
        if ( !success )
        {
            throw cause;
        }
    }
    
    public static void truncateFile( File file, long position ) throws IOException
    {
        RandomAccessFile access = new RandomAccessFile( file, "rw" );
        try
        {
            truncateFile( access.getChannel(), position );
        }
        finally
        {
            access.close();
        }
    }

    private static void waitSome()
    {
        try
        {
            Thread.sleep( 500 );
        }
        catch ( InterruptedException ee )
        {
            Thread.interrupted();
        } // ok
        System.gc();
    }

    public static String fixSeparatorsInPath( String path )
    {
        String fileSeparator = System.getProperty( "file.separator" );
        if ( "\\".equals( fileSeparator ) )
        {
            path = path.replace( '/', '\\' );
        }
        else if ( "/".equals( fileSeparator ) )
        {
            path = path.replace( '\\', '/' );
        }
        return path;
    }
}
