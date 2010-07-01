/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.impl.PrimitiveUtils;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

abstract class LuceneCommand extends XaCommand
{
    private final IndexIdentifier indexId;
    private final long entityId;
    private final String key;
    private final String value;
    
    private static final byte ADD_COMMAND = (byte) 1;
    private static final byte REMOVE_COMMAND = (byte) 2;
    private static final byte CLEAR_COMMAND = (byte) 3;
    
    private static final byte NODE = (byte) 1;
    private static final byte RELATIONSHIP = (byte) 2;
    
    LuceneCommand( IndexIdentifier indexId, long entityId, String key, String value )
    {
        this.indexId = indexId;
        this.entityId = entityId;
        this.key = key;
        this.value = value;
    }
    
    LuceneCommand( CommandData data )
    {
        this.indexId = data.indexId;
        this.entityId = data.nodeId;
        this.key = data.key;
        this.value = data.value;
    }
    
    public IndexIdentifier getIndexIdentifier()
    {
        return indexId;
    }
    
    public long getEntityId()
    {
        return entityId;
    }
    
    public String getKey()
    {
        return key;
    }
    
    public String getValue()
    {
        return value;
    }
    
    @Override
    public void execute()
    {
        // TODO Auto-generated method stub
    }
    
    private byte indexClass()
    {
        if ( indexId.itemsClass.equals( Node.class ) )
        {
            return NODE;
        }
        else if ( indexId.itemsClass.equals( Relationship.class ) )
        {
            return RELATIONSHIP;
        }
        throw new RuntimeException( indexId.itemsClass.getName() );
    }

    @Override
    public void writeToFile( LogBuffer buffer ) throws IOException
    {
        buffer.put( getCommandValue() );
        buffer.put( indexClass() );
        char[] indexName = indexId.indexName.toCharArray();
        buffer.putInt( indexName.length );
        buffer.putLong( entityId );
        char[] key = this.key.toCharArray();
        buffer.putInt( key.length );
        char[] value = this.value.toCharArray();
        buffer.putInt( value.length );
        
        buffer.put( indexName );
        buffer.put( key );
        buffer.put( value );
    }
    
    protected abstract byte getCommandValue();
    
    static class AddCommand extends LuceneCommand
    {
        AddCommand( IndexIdentifier indexId, long entityId, String key, String value )
        {
            super( indexId, entityId, key, value );
        }
        
        AddCommand( CommandData data )
        {
            super( data );
        }

        @Override
        protected byte getCommandValue()
        {
            return ADD_COMMAND;
        }
    }
    
    static class RemoveCommand extends LuceneCommand
    {
        RemoveCommand( IndexIdentifier indexId, long nodeId, String key, String value )
        {
            super( indexId, nodeId, key, value );
        }
        
        RemoveCommand( CommandData data )
        {
            super( data );
        }

        @Override
        protected byte getCommandValue()
        {
            return REMOVE_COMMAND;
        }
    }

    static class ClearCommand extends LuceneCommand
    {
        ClearCommand( IndexIdentifier indexId )
        {
            super( indexId, -1L, "", "" );
        }
        
        ClearCommand( CommandData data )
        {
            super( data );
        }

        @Override
        protected byte getCommandValue()
        {
            return CLEAR_COMMAND;
        }
    }
    
    private static class CommandData
    {
        private final IndexIdentifier indexId;
        private final long nodeId;
        private final String key;
        private final String value;
        
        CommandData( IndexIdentifier indexId, long nodeId, String key, String value )
        {
            this.indexId = indexId;
            this.nodeId = nodeId;
            this.key = key;
            this.value = value;
        }
    }
    
    static CommandData readCommandData( ReadableByteChannel channel, 
        ByteBuffer buffer, LuceneDataSource dataSource ) throws IOException
    {
        buffer.clear(); buffer.limit( 21 );
        if ( channel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        
        byte cls = buffer.get();
        Class<?> itemsClass = null;
        if ( cls == NODE )
        {
            itemsClass = Node.class;
        }
        else if ( cls == RELATIONSHIP )
        {
            itemsClass = Relationship.class;
        }
        else
        {
            return null;
        }
        
        int indexNameLength = buffer.getInt();
        long entityId = buffer.getLong();
        int keyCharLength = buffer.getInt();
        int valueCharLength = buffer.getInt();

        String indexName = PrimitiveUtils.readString( channel, buffer, indexNameLength );
        if ( indexName == null )
        {
            return null;
        }
        
        String key = PrimitiveUtils.readString( channel, buffer, keyCharLength );
        if ( key == null )
        {
            return null;
        }

        String value = PrimitiveUtils.readString( channel, buffer, valueCharLength );
        if ( value == null )
        {
            return null;
        }
        IndexIdentifier identifier = new IndexIdentifier( itemsClass, indexName,
                dataSource.indexStore.get( indexName ) );
        return new CommandData( identifier, entityId, key, value );
    }
    
    static XaCommand readCommand( ReadableByteChannel channel, 
        ByteBuffer buffer, LuceneDataSource dataSource ) throws IOException
    {
        buffer.clear(); buffer.limit( 1 );
        if ( channel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte commandType = buffer.get();
        CommandData data = readCommandData( channel, buffer, dataSource );
        if ( data == null )
        {
            return null;
        }
        switch ( commandType )
        {
            case ADD_COMMAND: return new AddCommand( data ); 
            case REMOVE_COMMAND: return new RemoveCommand( data );
            case CLEAR_COMMAND: return new ClearCommand( data );
            default:
                throw new IOException( "Unknown command type[" + 
                    commandType + "]" );
        }
    }
}
