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

package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.index.impl.PrimitiveUtils;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

abstract class LuceneCommand extends XaCommand
{
    private static final byte ADD_COMMAND = (byte) 1;
    private static final byte REMOVE_COMMAND = (byte) 2;
    private static final byte DELETE_COMMAND = (byte) 3;
    private static final byte CREATE_INDEX_COMMAND = (byte) 4;
    
    public static final byte NODE = (byte) 1;
    public static final byte RELATIONSHIP = (byte) 2;
    
    private static final byte VALUE_TYPE_INT = (byte) 1;
    private static final byte VALUE_TYPE_LONG = (byte) 2;
    private static final byte VALUE_TYPE_FLOAT = (byte) 3;
    private static final byte VALUE_TYPE_DOUBLE = (byte) 4;
    private static final byte VALUE_TYPE_STRING = (byte) 5;
    
    final IndexIdentifier indexId;
    final Object entityId;
    final String key;
    final Object value;
    final byte type;
    private final byte entityType;
    
    LuceneCommand( IndexIdentifier indexId, byte entityType, Object entityId, String key, Object value, byte type )
    {
        this.indexId = indexId;
        this.entityType = entityType;
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
        return this.entityType;
    }

    @Override
    public void writeToFile( LogBuffer buffer ) throws IOException
    {
        buffer.put( type );
        buffer.put( indexClass() );
        char[] indexName = indexId.indexName.toCharArray();
        buffer.putInt( indexName.length );
        long id = entityId instanceof Long ? (Long) entityId : ((RelationshipId)entityId).id;
        buffer.putLong( id );
        char[] key = this.key.toCharArray();
        buffer.putInt( key.length );
        
        byte valueType = 0;
        if ( value instanceof Number )
        {
            if ( value instanceof Float )
            {
                valueType = VALUE_TYPE_FLOAT;
            }
            else if ( value instanceof Double )
            {
                valueType = VALUE_TYPE_DOUBLE;
            }
            else if ( value instanceof Long )
            {
                valueType = VALUE_TYPE_LONG;
            }
            else
            {
                valueType = VALUE_TYPE_INT;
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
            Number number = (Number) value;
            switch ( valueType )
            {
            case VALUE_TYPE_FLOAT:
                buffer.putInt( Float.floatToRawIntBits( number.floatValue() ) );
                break;
            case VALUE_TYPE_DOUBLE:
                buffer.putLong( Double.doubleToRawLongBits( number.doubleValue() ) );
                break;
            case VALUE_TYPE_LONG:
                buffer.putLong( number.longValue() );
                break;
            case VALUE_TYPE_INT:
                buffer.putInt( number.intValue() );
                break;
            default:
                throw new Error( "Should not reach here." );
            }
        }
    }
    
    private static void writeLengthAndString( LogBuffer buffer, String string ) throws IOException
    {
        char[] chars = string.toCharArray();
        buffer.putInt( chars.length );
        buffer.put( chars );
    }
    
    static class AddCommand extends LuceneCommand
    {
        AddCommand( IndexIdentifier indexId, byte entityType, Object entityId, String key, Object value )
        {
            super( indexId, entityType, entityId, key, value, ADD_COMMAND );
        }
    }
    
    static class AddRelationshipCommand extends LuceneCommand
    {
        AddRelationshipCommand( IndexIdentifier indexId, byte entityType, RelationshipId entityId, String key,
                Object value )
        {
            super( indexId, entityType, entityId, key, value, ADD_COMMAND );
        }
        
        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            super.writeToFile( buffer );
            buffer.putLong( ((RelationshipId) entityId).startNode );
            buffer.putLong( ((RelationshipId) entityId).endNode );
        }
    }
    
    static class RemoveCommand extends LuceneCommand
    {
        RemoveCommand( IndexIdentifier indexId, byte entityType, Object entityId, String key, Object value )
        {
            super( indexId, entityType, entityId, key, value, REMOVE_COMMAND );
        }
    }

    static class DeleteCommand extends LuceneCommand
    {
        DeleteCommand( IndexIdentifier indexId )
        {
            super( indexId, (byte)0, -1L, "", "", DELETE_COMMAND );
        }
    }
    
    static class CreateIndexCommand extends LuceneCommand
    {
        static final IndexIdentifier FAKE_IDENTIFIER = new IndexIdentifier(
                (byte) 9, null, "create index", null );
        private final String name;
        private final Map<String, String> config;

        CreateIndexCommand( String name, Map<String, String> config )
        {
            super( FAKE_IDENTIFIER, (byte) 0, -1L, null, null, CREATE_INDEX_COMMAND );
            this.name = name;
            this.config = config;
        }
        
        public String getName()
        {
            return name;
        }

        public Map<String, String> getConfig()
        {
            return config;
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( type );
            writeLengthAndString( buffer, name );
            buffer.putInt( config.size() );
            for ( Map.Entry<String, String> entry : config.entrySet() )
            {
                writeLengthAndString( buffer, entry.getKey() );
                writeLengthAndString( buffer, entry.getValue() );
            }
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
        
        if ( commandType == CREATE_INDEX_COMMAND )
        {
            buffer.clear();
            String name = PrimitiveUtils.readLengthAndString( channel, buffer );
            if ( name == null )
            {
                return null;
            }
            int size = PrimitiveUtils.readInt( channel, buffer );
            Map<String, String> config = new HashMap<String, String>();
            for ( int i = 0; i < size; i++ )
            {
                String key = PrimitiveUtils.readLengthAndString( channel, buffer );
                String value = PrimitiveUtils.readLengthAndString( channel, buffer );
                if ( key == null || value == null )
                {
                    return null;
                }
                config.put( key, value );
            }
            return new CreateIndexCommand( name, config );
        }
        else
        {
            // Read the command data
            buffer.clear(); buffer.limit( 18 );
            if ( channel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            
            byte cls = buffer.get();
            EntityType entityType = null;
            if ( cls == NODE )
            {
                entityType = dataSource.nodeEntityType;
            }
            else if ( cls == RELATIONSHIP )
            {
                entityType = dataSource.relationshipEntityType;
            }
            else
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
                case VALUE_TYPE_INT: value = PrimitiveUtils.readInt( channel, buffer ); break;
                case VALUE_TYPE_LONG: value = PrimitiveUtils.readLong( channel, buffer ); break;
                case VALUE_TYPE_FLOAT: value = PrimitiveUtils.readFloat( channel, buffer ); break;
                case VALUE_TYPE_DOUBLE: value = PrimitiveUtils.readDouble( channel, buffer ); break;
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
            
            IndexIdentifier identifier = new IndexIdentifier( cls, entityType, indexName,
                    dataSource.indexStore.get( indexName ) );
            
            switch ( commandType )
            {
                case ADD_COMMAND: return cls == NODE ?
                        new AddCommand( identifier, cls, entityId, key, value ) :
                        new AddRelationshipCommand( identifier, cls,
                                new RelationshipId( entityId, startNodeId, endNodeId ), key, value );
                case REMOVE_COMMAND: return new RemoveCommand( identifier, cls, entityId, key, value );
                case DELETE_COMMAND: return new DeleteCommand( identifier );
                default:
                    throw new IOException( "Unknown command type[" + 
                        commandType + "]" );
            }
        }
    }
}
