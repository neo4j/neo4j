package org.neo4j.kernel.impl.index;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

public class IndexStore
{
    private static final byte[] MAGICK = new byte[] { 'n', 'e', 'o', '4', 'j', '-', 'i', 'n', 'd', 'e', 'x' };
    private static final int VERSION = 1;
    
    private final File file;
    private final Map<String, Map<String, String>> nodeConfig = new HashMap<String, Map<String,String>>();
    private final Map<String, Map<String, String>> relConfig = new HashMap<String, Map<String,String>>();
    private ByteBuffer dontUseBuffer = ByteBuffer.allocate( 100 );
    
    public IndexStore( String graphDbStoreDir )
    {
        this.file = new File( new File( graphDbStoreDir ), "index.db" );
        read();
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
        if ( !file.exists() )
        {
            return;
        }
        
        FileChannel channel = null;
        try
        {
            channel = new RandomAccessFile( file, "r" ).getChannel();
            Integer version = tryToReadVersion( channel );
            if ( version == null )
            {
                close( channel );
                channel = new RandomAccessFile( file, "r" ).getChannel();
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

    private Map<String, Map<String, String>> readMap( FileChannel channel,
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
        return map;
    }
    
    private Integer tryToReadVersion( ReadableByteChannel channel ) throws IOException
    {
        byte[] array = IoPrimitiveUtils.readBytes( channel, new byte[MAGICK.length] );
        return array != null ? readNextInt( channel ) : null;
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

    private Integer readNextInt( ReadableByteChannel channel ) throws IOException
    {
        return IoPrimitiveUtils.readInt( channel, buffer( 4 ) );
    }

    private String readNextString( ReadableByteChannel channel ) throws IOException
    {
        return IoPrimitiveUtils.readLengthAndString( channel, buffer( 100 ) );
    }

    public synchronized Map<String, String> get( Class<? extends PropertyContainer> cls, String indexName )
    {
        return map( cls ).get( indexName );
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

    public Map<String, Map<String, String>> asMap( Class<? extends PropertyContainer> cls )
    {
        return new HashMap<String, Map<String,String>>( map( cls ) );
    }
    
    public synchronized void remove( Class<? extends PropertyContainer> cls, String indexName )
    {
        if ( map( cls ).remove( indexName ) == null )
        {
            throw new RuntimeException( "Index config for '" + indexName + "' not found" );
        }
        write();
    }
    
    public synchronized boolean setIfNecessary( Class<? extends PropertyContainer> cls,
            String name, Map<String, String> config )
    {
        Map<String, Map<String, String>> map = map( cls );
        if ( map.containsKey( name ) )
        {
            return false;
        }
        map.put( name, config );
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
            channel.write( ByteBuffer.wrap( MAGICK ) );
            IoPrimitiveUtils.writeInt( channel, buffer( 4 ), VERSION );
            writeMap( channel, nodeConfig );
            writeMap( channel, relConfig );
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

    private void writeMap( FileChannel channel, Map<String, Map<String, String>> map ) throws IOException
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

    private void writeInt( FileChannel channel, int value ) throws IOException
    {
        IoPrimitiveUtils.writeInt( channel, buffer( 4 ), value );
    }
    
    private void writeString( FileChannel channel, String value ) throws IOException
    {
        IoPrimitiveUtils.writeLengthAndString( channel, buffer( 200 ), value );
    }
}
