/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.onlinebackup;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneFulltextIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Util
{
    static final String FILE_SEP = System.getProperty( "file.separator" );

    static void copyDir( String source, String dest )
    {
        try
        {
            File destination = new File( dest );
            if ( !destination.exists() )
            {
                if ( !destination.mkdir() )
                {
                    System.out
                        .println( "Couldn't create destination directory: "
                            + destination );
                }
            }
            File directory = new File( source );
            if ( !directory.exists() || !directory.isDirectory() )
            {
                return;
            }
            String[] contents = directory.list();
            for ( int i = 0; i < contents.length; i++ )
            {
                File file = new File( source + FILE_SEP + contents[i] );
                if ( file.isDirectory() )
                {
                    copyDir( file.getAbsolutePath(), dest + FILE_SEP
                        + contents[i] );
                }
                if ( !file.isFile() || !file.canRead() )
                {
                    continue;
                }
                FileChannel in = new FileInputStream( file ).getChannel();
                FileChannel out = new FileOutputStream( dest + FILE_SEP
                    + contents[i] ).getChannel();
                in.transferTo( 0, in.size(), out );
                in.close();
                out.close();
            }
        }
        catch ( Exception e )
        {
            fail( "couldn't copy files as required" );
            e.printStackTrace();
        }
    }

    static void copyLogs( String source, String dest )
    {
        try
        {
            File directory = new File( source );
            if ( !directory.exists() || !directory.isDirectory() )
            {
                return;
            }
            String[] contents = directory.list();
            for ( int i = 0; i < contents.length; i++ )
            {
                File file = new File( source + FILE_SEP + contents[i] );
                if ( file.isDirectory() )
                {
                    copyLogs( file.getAbsolutePath(), dest + FILE_SEP
                        + contents[i] );
                }
                int index = contents[i].lastIndexOf( "." );
                if ( index == -1 )
                {
                    continue;
                }
                String end = contents[i].substring( index + 1 );
                if ( !file.isFile() || !file.canRead() || 
                        !end.startsWith( "v" ) )
                {
                    continue;
                }
                
                FileChannel in = new FileInputStream( file ).getChannel();
                FileChannel out = new FileOutputStream( dest + FILE_SEP
                    + contents[i] ).getChannel();
                in.transferTo( 0, in.size(), out );
                in.close();
                out.close();
            }
        }
        catch ( Exception e )
        {
            fail( "couldn't copy files as required" );
            e.printStackTrace();
        }
    }

    static boolean deleteDir( File directory )
    {
        if ( directory.isDirectory() )
        {
            String[] contents = directory.list();
            for ( int i = 0; i < contents.length; i++ )
            {
                if ( !deleteDir( new File( directory, contents[i] ) ) )
                {
                    return false;
                }
            }
        }
        return directory.delete();
    }

    static EmbeddedGraphDatabase startGraphDbInstance( String location )
    {
        File file = new File( location );
        return new EmbeddedGraphDatabase( file.getAbsolutePath() );
    }

    static EmbeddedGraphDatabase startGraphDbInstance( String location,
            Map<String, String> configuration )
    {
        File file = new File( location );
        return new EmbeddedGraphDatabase( file.getAbsolutePath(), configuration );
    }

    static void stopGraphDb( EmbeddedGraphDatabase graphDb )
    {
        graphDb.shutdown();
    }

    static void stopGraphDb( EmbeddedGraphDatabase graphDb, IndexService indexService )
    {
        indexService.shutdown();
        stopGraphDb( graphDb );
    }

    static void stopGraphDb( EmbeddedGraphDatabase graphDb,
            IndexService indexService, LuceneFulltextIndexService fulltextIndex )
    {
        fulltextIndex.shutdown();
        stopGraphDb( graphDb, indexService );
    }
}
