/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read2bMap;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.read3bLengthAndString;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readBytes;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readDouble;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readFloat;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readInt;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readLong;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readShort;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write2bLengthAndString;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write3bLengthAndString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

/**
 * Created from {@link IndexDefineCommand} or read from a logical log.
 * Contains all the different types of commands that an {@link Index} need
 * to support.
 */
public abstract class IndexCommand extends XaCommand
{
    static final byte DEFINE_COMMAND = (byte) 0;
    static final byte ADD_COMMAND = (byte) 1;
    static final byte ADD_RELATIONSHIP_COMMAND = (byte) 2;
    static final byte REMOVE_COMMAND = (byte) 3;
    static final byte DELETE_COMMAND = (byte) 4;
    static final byte CREATE_COMMAND = (byte) 5;
    
    public static final byte NODE = (byte) 0;
    public static final byte RELATIONSHIP = (byte) 1;
    
    private static final byte VALUE_TYPE_NULL = (byte) 0;
    private static final byte VALUE_TYPE_SHORT = (byte) 1;
    private static final byte VALUE_TYPE_INT = (byte) 2;
    private static final byte VALUE_TYPE_LONG = (byte) 3;
    private static final byte VALUE_TYPE_FLOAT = (byte) 4;
    private static final byte VALUE_TYPE_DOUBLE = (byte) 5;
    private static final byte VALUE_TYPE_STRING = (byte) 6;
    
    private final byte commandType;
    private final byte indexNameId;
    private final byte entityType;
    private final long entityId;
    private final byte keyId;
    private final byte valueType;
    private final Object value;
    
    IndexCommand( byte commandType, byte indexNameId, byte entityType, long entityId, byte keyId, Object value )
    {
        this.commandType = commandType;
        this.indexNameId = indexNameId;
        this.entityType = entityType;
        this.entityId = entityId;
        this.keyId = keyId;
        this.value = value;
        this.valueType = valueTypeOf( value );
    }
    
    public byte getIndexNameId()
    {
        return indexNameId;
    }
    
    public byte getEntityType()
    {
        return entityType;
    }
    
    public long getEntityId()
    {
        return entityId;
    }
    
    public byte getKeyId()
    {
        return keyId;
    }
    
    public Object getValue()
    {
        return value;
    }
    
    @Override
    public void execute()
    {
    }
    
    @Override
    public void writeToFile( LogBuffer buffer ) throws IOException
    {
        /* c: commandType
         * e: entityType
         * n: indexNameId
         * k: keyId
         * i: entityId
         * v: value type
         * u: value
         * x: 0=entityId needs 4b, 1=entityId needs 8b
         * y: 0=startNode needs 4b, 1=startNode needs 8b
         * z: 0=endNode needs 4b, 1=endNode needs 8b
         * 
         * [cccv,vvex][yznn,nnnn][kkkk,kkkk]
         * [iiii,iiii] x 4 or 8
         * (either string value)
         * [llll,llll][llll,llll][llll,llll][string chars...]
         * (numeric value)
         * [uuuu,uuuu] x 2-8 (depending on value type)
         */
        
        writeHeader( buffer );
        putIntOrLong( buffer, entityId );
        
        // Value
        switch ( valueType )
        {
        case VALUE_TYPE_STRING: write3bLengthAndString( buffer, value.toString() ); break;
        case VALUE_TYPE_SHORT: buffer.putShort( ((Number) value).shortValue() ); break;
        case VALUE_TYPE_INT: buffer.putInt( ((Number) value).intValue() ); break;
        case VALUE_TYPE_LONG: buffer.putLong( ((Number) value).longValue() ); break;
        case VALUE_TYPE_FLOAT: buffer.putFloat( ((Number) value).floatValue() ); break;
        case VALUE_TYPE_DOUBLE: buffer.putDouble( ((Number) value).doubleValue() ); break;
        case VALUE_TYPE_NULL: break;
        default: throw new RuntimeException( "Unknown value type " + valueType );
        }
    }

    protected void writeHeader( LogBuffer buffer ) throws IOException
    {
        buffer.put( (byte)((commandType<<5) | (valueType<<2) | (entityType<<1) | (needsLong( entityId ))) );
        buffer.put( (byte)((startNodeNeedsLong()<<7) | (endNodeNeedsLong()<<6) | (indexNameId)) );
        buffer.put( keyId );
    }
    
    protected static void putIntOrLong( LogBuffer buffer, long id ) throws IOException
    {
        if ( needsLong( id ) == 1 )
        {
            buffer.putLong( id );
        }
        else
        {
            buffer.putInt( (int)id );
        }
    }

    protected static byte needsLong( long value )
    {
        return value > Integer.MAX_VALUE ? (byte)1 : (byte)0;
    }
    
    protected byte startNodeNeedsLong()
    {
        return 0;
    }
    
    protected byte endNodeNeedsLong()
    {
        return 0;
    }
    
    private static byte valueTypeOf( Object value )
    {
        byte valueType = 0;
        if ( value == null )
        {
            valueType = VALUE_TYPE_NULL;
        }
        else if ( value instanceof Number )
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
            else if ( value instanceof Short )
            {
                valueType = VALUE_TYPE_SHORT;
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
        return valueType;
    }
    
    public boolean isConsideredNormalWriteCommand()
    {
        return true;
    }
    
    public static class AddCommand extends IndexCommand
    {
        AddCommand( byte indexNameId, byte entityType, long entityId, byte keyId, Object value )
        {
            super( ADD_COMMAND, indexNameId, entityType, entityId, keyId, value );
        }
    }
    
    public static class AddRelationshipCommand extends IndexCommand
    {
        private final long startNode;
        private final long endNode;

        AddRelationshipCommand( byte indexNameId, byte entityType, long entityId, byte keyId,
                Object value, long startNode, long endNode )
        {
            super( ADD_RELATIONSHIP_COMMAND, indexNameId, entityType, entityId, keyId, value );
            this.startNode = startNode;
            this.endNode = endNode;
        }
        
        public long getStartNode()
        {
            return startNode;
        }
        
        public long getEndNode()
        {
            return endNode;
        }
        
        @Override
        protected byte startNodeNeedsLong()
        {
            return needsLong( startNode );
        }
        
        @Override
        protected byte endNodeNeedsLong()
        {
            return needsLong( endNode );
        }
        
        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            super.writeToFile( buffer );
            putIntOrLong( buffer, startNode );
            putIntOrLong( buffer, endNode );
        }

        @Override
        public int hashCode()
        {
            int result = (int) (startNode ^ (startNode >>> 32));
            result = 31 * result + (int) (endNode ^ (endNode >>> 32));
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( !super.equals( obj ) )
            {
                return false;
            }
            AddRelationshipCommand other = (AddRelationshipCommand) obj;
            return startNode == other.startNode && endNode == other.endNode;
        }
    }
    
    public static class RemoveCommand extends IndexCommand
    {
        RemoveCommand( byte indexNameId, byte entityType, long entityId, byte keyId, Object value )
        {
            super( REMOVE_COMMAND, indexNameId, entityType, entityId, keyId, value );
        }
    }

    public static class DeleteCommand extends IndexCommand
    {
        DeleteCommand( byte indexNameId, byte entityType )
        {
            super( DELETE_COMMAND, indexNameId, entityType, 0L, (byte)0, null );
        }
        
        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            writeHeader( buffer );
        }
        
        @Override
        public boolean isConsideredNormalWriteCommand()
        {
            return false;
        }
    }
    
    public static class CreateCommand extends IndexCommand
    {
        private final Map<String, String> config;

        CreateCommand( byte indexNameId, byte entityType, Map<String, String> config )
        {
            super( CREATE_COMMAND, indexNameId, entityType, 0L, (byte)0, null ); 
            this.config = config;
        }
        
        public Map<String, String> getConfig()
        {
            return config;
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            writeHeader( buffer );
            buffer.putShort( (short)config.size() );
            for ( Map.Entry<String, String> entry : config.entrySet() )
            {
                write2bLengthAndString( buffer, entry.getKey() );
                write2bLengthAndString( buffer, entry.getValue() );
            }
        }
        
        @Override
        public boolean isConsideredNormalWriteCommand()
        {
            return false;
        }

        @Override
        public int hashCode()
        {
            return config != null ? config.hashCode() : 0;
        }

        @Override
        public boolean equals( Object obj )
        {
            return super.equals( obj ) && config.equals( ((CreateCommand)obj).config );
        }
    }
    
    private static Object readValue( byte valueType, ReadableByteChannel channel, ByteBuffer buffer )
            throws IOException
    {
        switch ( valueType )
        {
        case VALUE_TYPE_NULL: return null;
        case VALUE_TYPE_SHORT: return readShort( channel, buffer );
        case VALUE_TYPE_INT: return readInt( channel, buffer );
        case VALUE_TYPE_LONG: return readLong( channel, buffer );
        case VALUE_TYPE_FLOAT: return readFloat( channel, buffer );
        case VALUE_TYPE_DOUBLE: return readDouble( channel, buffer );
        case VALUE_TYPE_STRING: return read3bLengthAndString( channel, buffer );
        default: throw new RuntimeException( "Unknown value type " + valueType );
        }
    }
    
    public static XaCommand readCommand( ReadableByteChannel channel, ByteBuffer buffer ) throws IOException
    {
        byte[] headerBytes = readBytes( channel, new byte[3] );
        if ( headerBytes == null ) return null;
        
        byte commandType = (byte)((headerBytes[0] & 0xE0) >> 5);
        byte valueType = (byte)((headerBytes[0] & 0x1C) >> 2);
        byte entityType = (byte)((headerBytes[0] & 0x2) >> 1);
        boolean entityIdNeedsLong = (headerBytes[0] & 0x1) > 0;
        byte indexNameId = (byte)(headerBytes[1] & 0x3F);
        byte keyId = headerBytes[2];
        
        switch ( commandType )
        {
        case DEFINE_COMMAND:
            Map<String, Byte> indexNames = IndexDefineCommand.readMap( channel, buffer );
            Map<String, Byte> keys = IndexDefineCommand.readMap( channel, buffer );
            if ( indexNames == null || keys == null ) return null;
            return new IndexDefineCommand( indexNames, keys );
        case CREATE_COMMAND:
            Map<String, String> config = read2bMap( channel, buffer );
            if ( config == null ) return null;
            return new CreateCommand( indexNameId, entityType, config );
        case DELETE_COMMAND:
            return new DeleteCommand( indexNameId, entityType );
        case ADD_COMMAND: case REMOVE_COMMAND: case ADD_RELATIONSHIP_COMMAND:
            Number entityId = entityIdNeedsLong ? (Number)readLong( channel, buffer ) : (Number)readInt( channel, buffer );
            if ( entityId == null ) return null;
            Object value = readValue( valueType, channel, buffer );
            if ( valueType != VALUE_TYPE_NULL && value == null ) return null;
            if ( commandType == ADD_COMMAND )
            {
                return new AddCommand( indexNameId, entityType, entityId.longValue(), keyId, value );
            }
            else if ( commandType == REMOVE_COMMAND )
            {
                return new RemoveCommand( indexNameId, entityType, entityId.longValue(), keyId, value );
            }
            else
            {
                boolean startNodeNeedsLong = (headerBytes[1] & 0x8) > 0;
                boolean endNodeNeedsLong = (headerBytes[1] & 0x40) > 0;
                Number startNode = startNodeNeedsLong ? (Number)readLong( channel, buffer ) : (Number)readInt( channel, buffer );
                Number endNode = endNodeNeedsLong ? (Number)readLong( channel, buffer ) : (Number)readInt( channel, buffer );
                if ( startNode == null || endNode == null ) return null;
                return new AddRelationshipCommand( indexNameId, entityType, entityId.longValue(),
                        keyId, value, startNode.longValue(), endNode.longValue() );
            }
        default: throw new RuntimeException( "Unknown command type " + commandType );
        }
    }
    
    @Override
    public boolean equals( Object obj )
    {
        IndexCommand other = (IndexCommand) obj;
        boolean equals = commandType == other.commandType &&
                entityType == other.entityType &&
                indexNameId == other.indexNameId &&
                keyId == other.keyId &&
                valueType == other.valueType;
        if ( !equals )
        {
            return false;
        }
        
        return value == null ? other.value == null : value.equals( other.value );
    }
}
