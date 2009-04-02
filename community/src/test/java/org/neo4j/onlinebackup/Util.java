package org.neo4j.onlinebackup;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.util.index.IndexService;

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

    static EmbeddedNeo startNeoInstance( String location )
    {
        File file = new File( location );
        return new EmbeddedNeo( file.getAbsolutePath() );
    }

    static void stopNeo( EmbeddedNeo neo )
    {
        neo.shutdown();
    }

    static void stopNeo( EmbeddedNeo neo, IndexService indexService )
    {
        indexService.shutdown();
        stopNeo( neo );
    }
}
