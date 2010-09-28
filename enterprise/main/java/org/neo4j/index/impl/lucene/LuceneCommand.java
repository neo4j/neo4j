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
import org.neo4j.kernel.impl.util.BufferNumberPutter;

abstract class LuceneCommand extends XaCommand
{
    private static final byte ADD_COMMAND = (byte) 1;
    private static final byte REMOVE_COMMAND = (byte) 2;
    private static final byte CLEAR_COMMAND = (byte) 3;
    
    private static final byte NODE = (byte) 1;
    private static final byte RELATIONSHIP = (byte) 2;
    
    private static final byte VALUE_TYPE_INT = (byte) 1;
    private static final byte VALUE_TYPE_LONG = (byte) 2;
    private static final byte VALUE_TYPE_FLOAT = (byte) 3;
    private static final byte VALUE_TYPE_DOUBLE = (byte) 4;
    private static final byte VALUE_TYPE_STRING = (byte) 5;
    
    final IndexIdentifier indexId;
    final long entityId;
    final String key;
    final Object value;
    final byte type;
    
    LuceneCommand( IndexIdentifier indexId, long entityId, String key, Object value, byte type )
    {
        this.indexId = indexId;
        this.entityId = entityId;
        this.key = key;
        this.value = value;
        this.type = type;
    }
    
    @Override
    public void execute()
    {
        // TODO Auto-generated method stub
    }
    
    private byte indexClass()
    {
        if ( indexId.entityType.getType().equals( Node.class ) )
        {
            return NODE;
        }
        else if ( indexId.entityType.getType().equals( Relationship.class ) )
        {
            return RELATIONSHIP;
        }
        throw new RuntimeException( indexId.entityType.getType().getName() );
    }

    @Override
    public void writeToFile( LogBuffer buffer ) throws IOException
    {
        buffer.put( type );
        buffer.put( indexClass() );
        char[] indexName = indexId.indexName.toCharArray();
        buffer.putInt( indexName.length );
        buffer.putLong( entityId );
        char[] key = this.key.toCharArray();
        buffer.putInt( key.length );
        
        byte valueType = 0;
        BufferNumberPutter putter = null;
        if ( value instanceof Number )
        {
            if ( value instanceof Float )
            {
                valueType = VALUE_TYPE_FLOAT;
                putter = BufferNumberPutter.FLOAT;
            }
            else if ( value instanceof Double )
            {
                valueType = VALUE_TYPE_DOUBLE;
                putter = BufferNumberPutter.DOUBLE;
            }
            else if ( value instanceof Long )
            {
                valueType = VALUE_TYPE_LONG;
                putter = BufferNumberPutter.LONG;
            }
            else
            {
                valueType = VALUE_TYPE_INT;
                putter = BufferNumberPutter.INT;
            }
        }
        else
        {
            valueType = VALUE_TYPE_STRING;
            
        }
        buffer.put( valueType );
        
        buffer.put( indexName );
        buffer.put( key );
        if ( valueType == VALUE_TYPE_STRING )
        {
            char[] charValue = value.toString().toCharArray();
            buffer.putInt( charValue.length );
            buffer.put( charValue );
        }
        else
        {
            putter.put( buffer, (Number) value );
        }
    }
    
    static class AddCommand extends LuceneCommand
    {
        AddCommand( IndexIdentifier indexId, long entityId, String key, Object value )
        {
            super( indexId, entityId, key, value, ADD_COMMAND );
        }
    }
    
    static class AddRelationshipCommand extends LuceneCommand
    {
        private final long startNodeId;
        private final long endNodeId;

        AddRelationshipCommand( IndexIdentifier indexId, long entityId, String key,
                Object value, long startNodeId, long endNodeId )
        {
            super( indexId, entityId, key, value, ADD_COMMAND );
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
        }
        
        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            super.writeToFile( buffer );
            buffer.putLong( startNodeId );
            buffer.putLong( endNodeId );
        }
    }
    
    static class RemoveCommand extends LuceneCommand
    {
        RemoveCommand( IndexIdentifier indexId, long nodeId, String key, Object value )
        {
            super( indexId, nodeId, key, value, REMOVE_COMMAND );
        }
    }

    static class ClearCommand extends LuceneCommand
    {
        ClearCommand( IndexIdentifier indexId )
        {
            super( indexId, -1L, "", "", CLEAR_COMMAND );
        }
    }
    
    static XaCommand readCommand( ReadableByteChannel channel, 
        ByteBuffer buffer, LuceneDataSource dataSource ) throws IOException
    {
        // Read what type of command it is
        buffer.clear(); buffer.limit( 1 );
        if ( channel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte commandType = buffer.get();
        
        // Read the command data
        buffer.clear(); buffer.limit( 18 );
        if ( channel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        
        byte cls = buffer.get();
        if ( cls != NODE && cls != RELATIONSHIP )
        {
            return null;
        }
        
        int indexNameLength = buffer.getInt();
        long entityId = buffer.getLong();
        int keyCharLength = buffer.getInt();
        byte valueType = buffer.get();

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

        Object value = null;
        if ( valueType >= VALUE_TYPE_INT && valueType <= VALUE_TYPE_DOUBLE )
        {
            switch ( valueType )
            {
            case VALUE_TYPE_INT: value = buffer.getInt(); break;
            case VALUE_TYPE_LONG: value = buffer.getLong(); break;
            case VALUE_TYPE_FLOAT: value = buffer.getFloat(); break;
            case VALUE_TYPE_DOUBLE: value = buffer.getDouble(); break;
            }
        }
        else if ( valueType == VALUE_TYPE_STRING )
        {
            value = PrimitiveUtils.readLengthAndString( channel, buffer );
        }
        if ( value == null )
        {
            return null;
        }
        
        Long startNodeId = null;
        Long endNodeId = null;
        if ( commandType == ADD_COMMAND && cls == RELATIONSHIP )
        {
            startNodeId = PrimitiveUtils.readLong( channel, buffer );
            endNodeId = PrimitiveUtils.readLong( channel, buffer );
            if ( startNodeId == null || endNodeId == null )
            {
                return null;
            }
        }
        
        // TODO
        IndexIdentifier identifier = new IndexIdentifier( null, indexName,
                dataSource.indexStore.get( indexName ) );
        
        switch ( commandType )
        {
            case ADD_COMMAND: return cls == NODE ?
                    new AddCommand( identifier, entityId, key, value ) :
                    new AddRelationshipCommand( identifier, entityId, key, value,
                            startNodeId, endNodeId );
            case REMOVE_COMMAND: return new RemoveCommand( identifier, entityId, key, value );
            case CLEAR_COMMAND: return new ClearCommand( identifier );
            default:
                throw new IOException( "Unknown command type[" + 
                    commandType + "]" );
        }
    }
}
