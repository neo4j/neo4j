package org.neo4j.bench.cases.memory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

// From Thobes java agent lib: https://github.com/thobe/java-agent/
class JarCreator
{
    private final Manifest manifest;

    JarCreator()
    {
        this( new Manifest() );
    }

    JarCreator( Manifest manifest )
    {
        manifest.getMainAttributes().put( Attributes.Name.MANIFEST_VERSION, "1.0" );
        this.manifest = manifest;
    }

    public String createTemporaryJar( String sourceFile )
    {
        File source = new File( sourceFile );
        try
        {
            File target = File.createTempFile( getClass().getSimpleName(), ".jar" );
            JarOutputStream stream = new JarOutputStream( new FileOutputStream( target ), manifest );
            try
            {
                if ( source.isFile() )
                {
                    try
                    {
                        copy( new JarFile( source ), stream );
                    }
                    catch ( IOException e )
                    {
                        throw new IllegalArgumentException(
                                String.format( "Cannot open source file (%s) for reading.", source ), e );
                    }
                }
                else
                {
                    create( "", source, stream );
                }
            }
            finally
            {
                stream.close();
            }
            return target.getAbsolutePath();
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Cannot create target file.", e );
        }
    }

    private void create( String path, File source, JarOutputStream target ) throws IOException
    {
        if ( source.isDirectory() )
        {
            if ( !path.isEmpty() )
            {
                path += '/';
                JarEntry entry = new JarEntry( path );
                entry.setTime( source.lastModified() );
                target.putNextEntry( entry );
                target.closeEntry();
            }
            for ( String child : source.list() )
            {
                create( path + child, new File( source, child ), target );
            }
        }
        else
        {
            JarEntry entry = new JarEntry( path );
            entry.setTime( source.lastModified() );
            target.putNextEntry( entry );
            FileInputStream input = new FileInputStream( source );
            try
            {
                copy( input, target );
            }
            finally
            {
                input.close();
            }
            target.closeEntry();
        }
    }

    private void copy( JarFile source, JarOutputStream target ) throws IOException
    {
        for ( Enumeration<JarEntry> entries = source.entries(); entries.hasMoreElements(); )
        {
            JarEntry entry = entries.nextElement();
            if ( "META-INF/MANIFEST.MF".equals( entry.getName() ) )
            {
                continue;
            }
            target.putNextEntry( entry );
            if ( !entry.isDirectory() )
            {
                copy( source.getInputStream( entry ), target );
            }
            target.closeEntry();
        }
    }

    private static void copy( InputStream source, OutputStream target ) throws IOException
    {
        BufferedInputStream in = new BufferedInputStream( source );
        try
        {
            byte[] buffer = new byte[1024];
            while ( true )
            {
                int count = in.read( buffer );
                if ( count == -1 )
                {
                    break;
                }
                target.write( buffer, 0, count );
            }
        }
        finally
        {
            in.close();
        }
    }
}
