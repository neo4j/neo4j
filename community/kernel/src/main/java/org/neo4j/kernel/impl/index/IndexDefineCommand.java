package org.neo4j.kernel.impl.index;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read2bLengthAndString;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readByte;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write2bLengthAndString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

public class IndexDefineCommand extends XaCommand
{
    private final AtomicInteger nextIndexNameId = new AtomicInteger();
    private final AtomicInteger nextKeyId = new AtomicInteger();
    private final Map<String, Byte> indexNameIdRange;
    private final Map<String, Byte> keyIdRange;
    
    public IndexDefineCommand()
    {
        indexNameIdRange = new HashMap<String, Byte>();
        keyIdRange = new HashMap<String, Byte>();
    }
    
    public IndexDefineCommand( Map<String, Byte> indexNames, Map<String, Byte> keys )
    {
        this.indexNameIdRange = indexNames;
        this.keyIdRange = keys;
    }
    
    public IndexCommand create( String indexName, Class<?> entityType, Map<String, String> config )
    {
        return new IndexCommand.CreateCommand( indexNameId( indexName ),
                entityTypeId( entityType ), config );
    }
    
    public IndexCommand add( String indexName, Class<?> entityType, long entityId, String key,
            Object value )
    {
        return new IndexCommand.AddCommand( indexNameId( indexName ), entityTypeId( entityType ),
                entityId, keyId( key ), value );
    }
    
    public IndexCommand addRelationship( String indexName, Class<?> entityType, long entityId, String key,
            Object value, long startNode, long endNode )
    {
        return new IndexCommand.AddRelationshipCommand( indexNameId( indexName ),
                entityTypeId( entityType ), entityId, keyId( key ), value, startNode, endNode );
    }
    
    public IndexCommand remove( String indexName, Class<?> entityType, long entityId,
            String key, Object value )
    {
        return new IndexCommand.RemoveCommand( indexNameId( indexName ), entityTypeId( entityType ),
                entityId, key != null ? keyId( key ) : 0, value );
    }
    
    public IndexCommand delete( String indexName, Class<?> entityType )
    {
        return new IndexCommand.DeleteCommand( indexNameId( indexName ), entityTypeId( entityType ) );
    }

    private byte entityTypeId( Class<?> entityType )
    {
        return entityType.equals( Relationship.class ) ? IndexCommand.RELATIONSHIP : IndexCommand.NODE;
    }
    
    private byte indexNameId( String indexName )
    {
        return id( indexName, indexNameIdRange, nextIndexNameId );
    }
    
    private byte keyId( String key )
    {
        return id( key, keyIdRange, nextKeyId );
    }

    private byte id( String key, Map<String, Byte> idRange, AtomicInteger nextId )
    {
        Byte id = idRange.get( key );
        if ( id == null )
        {
            id = Byte.valueOf( (byte) nextId.incrementAndGet() );
            idRange.put( key, id );
        }
        return id;
    }

    @Override
    public void execute()
    {
    }

    @Override
    public void writeToFile( LogBuffer buffer ) throws IOException
    {
        buffer.put( (byte)(IndexCommand.DEFINE_COMMAND << 5) );
        buffer.put( (byte)0 );
        buffer.put( (byte)0 );
        writeMap( indexNameIdRange, buffer );
        writeMap( keyIdRange, buffer );
    }
    
    static Map<String, Byte> readMap( ReadableByteChannel channel, ByteBuffer buffer )
            throws IOException
    {
        Byte size = readByte( channel, buffer );
        if ( size == null ) return null;
        Map<String, Byte> result = new HashMap<String, Byte>();
        for ( int i = 0; i < size; i++ )
        {
            String key = read2bLengthAndString( channel, buffer );
            Byte id = readByte( channel, buffer );
            if ( key == null || id == null ) return null;
            result.put( key, id );
        }
        return result;
    }

    private static void writeMap( Map<String, Byte> map, LogBuffer buffer ) throws IOException
    {
        buffer.put( (byte)map.size() );
        for ( Map.Entry<String, Byte> entry : map.entrySet() )
        {
            write2bLengthAndString( buffer, entry.getKey() );
            buffer.put( entry.getValue() );
        }
    }
    
    @Override
    public boolean equals( Object obj )
    {
        IndexDefineCommand other = (IndexDefineCommand) obj;
        return indexNameIdRange.equals( other.indexNameIdRange ) &&
                keyIdRange.equals( other.keyIdRange );
    }
}
