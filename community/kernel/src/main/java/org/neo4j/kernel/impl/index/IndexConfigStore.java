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
package org.neo4j.kernel.impl.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class IndexConfigStore extends LifecycleAdapter
{
    public static final String INDEX_DB_FILE_NAME = "index.db";
    private static final byte[] MAGICK = new byte[] { 'n', 'e', 'o', '4', 'j', '-', 'i', 'n', 'd', 'e', 'x' };
    private static final int VERSION = 1;

    private final File file;
    private final File oldFile;
    private final Map<String, Map<String, String>> nodeConfig = new ConcurrentHashMap<String, Map<String,String>>();
    private final Map<String, Map<String, String>> relConfig = new ConcurrentHashMap<String, Map<String,String>>();
    private ByteBuffer dontUseBuffer = ByteBuffer.allocate( 100 );
    private final FileSystemAbstraction fileSystem;

    public IndexConfigStore( File graphDbStoreDir, FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
        this.file = new File( graphDbStoreDir, INDEX_DB_FILE_NAME );
        this.oldFile = new File( file.getParentFile(), file.getName() + ".old" );
    }

    private ByteBuffer buffer( int size )
    {
        if ( dontUseBuffer.capacity() < size )
        {
            dontUseBuffer = ByteBuffer.allocate( size*2 );
        }
        return dontUseBuffer;
    }

    private void read()
    {
        File fileToReadFrom = fileSystem.fileExists( file ) ? file : oldFile;
        if ( !fileSystem.fileExists( fileToReadFrom ) )
        {
            return;
        }

        StoreChannel channel = null;
        try
        {
            channel = fileSystem.open( fileToReadFrom, "r" );
            Integer version = tryToReadVersion( channel );
            if ( version == null )
            {
                close( channel );
                channel = fileSystem.open( fileToReadFrom, "r" );
                // Legacy format, TODO
                readMap( channel, nodeConfig, version );
                relConfig.putAll( nodeConfig );
            }
            else if ( version < VERSION )
            {
                // ...add version upgrade code here
                throw new UnsupportedOperationException( "" + version );
            }
            else
            {
                readMap( channel, nodeConfig, readNextInt( channel ) );
                readMap( channel, relConfig, readNextInt( channel ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            close( channel );
        }
    }

    @Override
    public void init()
    {
        read();
    }

    @Override
    public void start() throws Throwable
    {
        // Refresh the read config
        nodeConfig.clear();
        relConfig.clear();
        read();
    }

    private Map<String, Map<String, String>> readMap( StoreChannel channel,
            Map<String, Map<String, String>> map, Integer sizeOrTillEof ) throws IOException
    {
        for ( int i = 0; sizeOrTillEof == null || i < sizeOrTillEof; i++ )
        {
            String indexName = readNextString( channel );
            if ( indexName == null )
            {
                break;
            }
            Integer propertyCount = readNextInt( channel );
            if ( propertyCount == null )
            {
                break;
            }
            Map<String, String> properties = new HashMap<String, String>();
            for ( int p = 0; p < propertyCount; p++ )
            {
                String key = readNextString( channel );
                if ( key == null )
                {
                    break;
                }
                String value = readNextString( channel );
                if ( value == null )
                {
                    break;
                }
                properties.put( key, value );
            }
            map.put( indexName, properties );
        }
        return Collections.unmodifiableMap( map );
    }

    private Integer tryToReadVersion( ReadableByteChannel channel ) throws IOException
    {
        byte[] array = IoPrimitiveUtils.readBytes( channel, new byte[MAGICK.length] );
        if ( !Arrays.equals( MAGICK, array ) )
        {
            return null;
        }
        return array != null ? readNextInt( channel ) : null;
    }

    private void close( StoreChannel channel )
    {
        if ( channel != null )
        {
            try
            {
                channel.close();
            }
            catch ( IOException e )
            {
                e.printStackTrace();
            }
        }
    }

    private Integer readNextInt( ReadableByteChannel channel ) throws IOException
    {
        return IoPrimitiveUtils.readInt( channel, buffer( 4 ) );
    }

    private String readNextString( ReadableByteChannel channel ) throws IOException
    {
        return IoPrimitiveUtils.readLengthAndString( channel, buffer( 100 ) );
    }

    public boolean has( Class<? extends PropertyContainer> cls, String indexName )
    {
        return map( cls ).containsKey( indexName );
    }

    public Map<String, String> get( Class<? extends PropertyContainer> cls, String indexName )
    {
        return map( cls ).get( indexName );
    }

    public String[] getNames( Class<? extends PropertyContainer> cls )
    {
        Map<String, Map<String, String>> indexMap = map( cls );
        return indexMap.keySet().toArray( new String[indexMap.size()] );
    }

    private Map<String, Map<String, String>> map( Class<? extends PropertyContainer> cls )
    {
        if ( cls.equals( Node.class ) )
        {
            return nodeConfig;
        }
        else if ( cls.equals( Relationship.class ) )
        {
            return relConfig;
        }
        throw new IllegalArgumentException( cls.toString() );
    }

    // Synchronized since only one thread are allowed to write at any given time
    public synchronized void remove( Class<? extends PropertyContainer> cls, String indexName )
    {
        if ( map( cls ).remove( indexName ) == null )
        {
            throw new RuntimeException( "Index config for '" + indexName + "' not found" );
        }
        write();
    }

    // Synchronized since only one thread are allowed to write at any given time
    public synchronized void set( Class<? extends PropertyContainer> cls,
            String name, Map<String, String> config )
    {
        map( cls ).put( name, Collections.unmodifiableMap( config ) );
        write();
    }

    // Synchronized since only one thread are allowed to write at any given time
    public synchronized boolean setIfNecessary( Class<? extends PropertyContainer> cls,
            String name, Map<String, String> config )
    {
        Map<String, Map<String, String>> map = map( cls );
        if ( map.containsKey( name ) )
        {
            return false;
        }
        map.put( name, Collections.unmodifiableMap( config ) );
        write();
        return true;
    }

    private void write()
    {
        // Write to a .tmp file
        File tmpFile = new File( this.file.getParentFile(), this.file.getName() + ".tmp" );
        write( tmpFile );

        // Make sure the .old file doesn't exist, then rename the current one to .old
        fileSystem.deleteFile( oldFile );
        try
        {
            if ( fileSystem.fileExists( file ) && !fileSystem.renameFile( file, oldFile ) )
            {
                throw new RuntimeException( "Couldn't rename " + file + " -> " + oldFile );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Couldn't rename " + file + " -> " + oldFile );
        }

        // Rename the .tmp file to the current name
        try
        {
            if ( !fileSystem.renameFile( tmpFile, this.file ) )
            {
                throw new RuntimeException( "Couldn't rename " + tmpFile + " -> " + file );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Couldn't rename " + tmpFile + " -> " + file );
        }
        fileSystem.deleteFile( oldFile );
    }

    private void write( File file )
    {
        StoreChannel channel = null;
        try
        {
            channel = fileSystem.open( file, "rw" );
            channel.write( ByteBuffer.wrap( MAGICK ) );
            IoPrimitiveUtils.writeInt( channel, buffer( 4 ), VERSION );
            writeMap( channel, nodeConfig );
            writeMap( channel, relConfig );
            channel.force( false );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            close( channel );
        }
    }

    private void writeMap( StoreChannel channel, Map<String, Map<String, String>> map ) throws IOException
    {
        IoPrimitiveUtils.writeInt( channel, buffer( 4 ), map.size() );
        for ( Map.Entry<String, Map<String, String>> entry : map.entrySet() )
        {
            writeString( channel, entry.getKey() );
            writeInt( channel, entry.getValue().size() );
            for ( Map.Entry<String, String> propertyEntry : entry.getValue().entrySet() )
            {
                writeString( channel, propertyEntry.getKey() );
                writeString( channel, propertyEntry.getValue() );
            }
        }
    }

    private void writeInt( StoreChannel channel, int value ) throws IOException
    {
        IoPrimitiveUtils.writeInt( channel, buffer( 4 ), value );
    }

    private void writeString( StoreChannel channel, String value ) throws IOException
    {
        IoPrimitiveUtils.writeLengthAndString( channel, buffer( 200 ), value );
    }
}
