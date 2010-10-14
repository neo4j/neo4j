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

package org.neo4j.index.impl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;

public class IndexStore
{
    private final Map<String, Map<String, String>> config;
    private final File file;
    private ByteBuffer dontUseBuffer = ByteBuffer.allocate( 100 );
    
    public IndexStore( String graphDbStoreDir )
    {
        this.file = new File( new File( graphDbStoreDir ), "index.db" );
        this.config = read();
    }
    
    private ByteBuffer buffer( int size )
    {
        if ( dontUseBuffer.capacity() < size )
        {
            dontUseBuffer = ByteBuffer.allocate( size*2 );
        }
        return dontUseBuffer;
    }
    
    private Map<String, Map<String, String>> read()
    {
        if ( !file.exists() )
        {
            return new HashMap<String, Map<String,String>>();
        }
        
        FileChannel channel = null;
        try
        {
            channel = new RandomAccessFile( file, "r" ).getChannel();
            Map<String, Map<String, String>> map = new HashMap<String, Map<String,String>>();
            while ( true )
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
                for ( int i = 0; i < propertyCount; i++ )
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
            return map;
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
    
    private void close( FileChannel channel )
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

    private Integer readNextInt(FileChannel channel) throws IOException
    {
        return PrimitiveUtils.readInt( channel, buffer( 4 ) );
    }

    private String readNextString(FileChannel channel) throws IOException
    {
        return PrimitiveUtils.readLengthAndString( channel, buffer( 100 ) );
    }

    public synchronized Map<String, String> get( String indexName )
    {
        return this.config.get( indexName );
    }
    
    public Map<String, Map<String, String>> asMap()
    {
        return new HashMap<String, Map<String,String>>( this.config );
    }
    
    public synchronized void remove( String name )
    {
        if ( this.config.remove( name ) == null )
        {
            throw new RuntimeException( "Index config for '" + name + "' not found" );
        }
        write();
    }
    
    public synchronized boolean setIfNecessary( String name, Map<String, String> config )
    {
        if ( this.config.containsKey( name ) )
        {
            return false;
        }
        this.config.put( name, config );
        write();
        return true;
    }
    
    private void write()
    {
        File tmpFile = new File( this.file.getParentFile(), this.file.getName() + ".tmp" );
        write( tmpFile );
        this.file.delete();
        tmpFile.renameTo( this.file );
    }
    
    private void write( File file )
    {
        FileChannel channel = null;
        try
        {
            channel = new RandomAccessFile( file, "rw" ).getChannel();
            for ( Map.Entry<String, Map<String, String>> entry : config.entrySet() )
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
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            close( channel );
        }
    }

    private void writeInt( FileChannel channel, int value ) throws IOException
    {
        PrimitiveUtils.writeInt( channel, buffer( 4 ), value );
    }
    
    private void writeString( FileChannel channel, String value ) throws IOException
    {
        PrimitiveUtils.writeLengthAndString( channel, buffer( 200 ), value );
    }
    
    public Pair<Map<String, String>, Boolean> getIndexConfig( String indexName,
            Map<String, String> userConfig, Map<?, ?> dbConfig, DefaultsFiller defaultsFiller )
    {
        // 1. Check stored config
        Map<String, String> storedConfig = get( indexName );
        userConfig = userConfig != null ? defaultsFiller.fill( userConfig ) : null;
        Map<String, String> configToUse = null;
        if ( storedConfig != null )
        {
            if ( userConfig != null && !defaultsFiller.fill( storedConfig ).equals( userConfig ) )
            {
                throw new IllegalArgumentException( "User index configuration:\n" +
                        userConfig + "\ndiffer from stored config:\n" + storedConfig +
                        "\nfor '" + indexName + "'" );
            }
            configToUse = storedConfig;
        }
        
        // 2. Check custom config
        if ( configToUse == null && userConfig != null )
        {
            configToUse = userConfig;
        }
        
        // 3. Check db config
        if ( configToUse == null )
        {
            String provider = null;
            if ( dbConfig != null )
            {
                provider = (String) dbConfig.get( "index." + indexName );
                if ( provider == null )
                {
                    provider = (String) dbConfig.get( "index" );
                }
            }
            
            // 4. Default to lucene
            if ( provider == null )
            {
                provider = "lucene";
            }
            configToUse = defaultsFiller.fill( MapUtil.stringMap( "provider", provider ) );
        }
        
        boolean created = setIfNecessary( indexName, configToUse );
        return new Pair<Map<String, String>, Boolean>( configToUse, created );
    }
}
