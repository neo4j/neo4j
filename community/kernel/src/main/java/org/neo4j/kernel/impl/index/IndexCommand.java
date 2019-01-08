/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.api.CommandVisitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.NeoCommandType;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write2bLengthAndString;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write3bLengthAndString;

/**
 * Created from {@link IndexDefineCommand} or read from a logical log.
 * Contains all the different types of commands that an {@link Index} need
 * to support.
 */
public abstract class IndexCommand extends Command
{
    public static final byte VALUE_TYPE_NULL = (byte) 0;
    public static final byte VALUE_TYPE_SHORT = (byte) 1;
    public static final byte VALUE_TYPE_INT = (byte) 2;
    public static final byte VALUE_TYPE_LONG = (byte) 3;
    public static final byte VALUE_TYPE_FLOAT = (byte) 4;
    public static final byte VALUE_TYPE_DOUBLE = (byte) 5;
    public static final byte VALUE_TYPE_STRING = (byte) 6;

    private byte commandType;
    protected int indexNameId;
    protected byte entityType;
    protected long entityId;
    protected int keyId;
    protected byte valueType;
    protected Object value;

    protected void init( byte commandType, int indexNameId, byte entityType, long entityId, int keyId, Object value )
    {
        this.commandType = commandType ;
        this.indexNameId = indexNameId;
        this.entityType = entityType;
        this.entityId = entityId;
        this.keyId = keyId;
        this.value = value;
        this.valueType = valueTypeOf( value );
    }

    public int getIndexNameId()
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

    public int getKeyId()
    {
        return keyId;
    }

    public Object getValue()
    {
        return value;
    }

    public byte startNodeNeedsLong()
    {
        return 0;
    }

    public byte endNodeNeedsLong()
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

    protected void writeToFile( WritableChannel channel ) throws IOException
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
        writeIndexCommandHeader( channel );
        putIntOrLong( channel, getEntityId() );
        // Value
        Object value = getValue();
        switch ( getValueType() )
        {
        case IndexCommand.VALUE_TYPE_STRING:
            write3bLengthAndString( channel, value.toString() );
            break;
        case IndexCommand.VALUE_TYPE_SHORT:
            channel.putShort( ((Number) value).shortValue() );
            break;
        case IndexCommand.VALUE_TYPE_INT:
            channel.putInt( ((Number) value).intValue() );
            break;
        case IndexCommand.VALUE_TYPE_LONG:
            channel.putLong( ((Number) value).longValue() );
            break;
        case IndexCommand.VALUE_TYPE_FLOAT:
            channel.putFloat( ((Number) value).floatValue() );
            break;
        case IndexCommand.VALUE_TYPE_DOUBLE:
            channel.putDouble( ((Number) value).doubleValue() );
            break;
        case IndexCommand.VALUE_TYPE_NULL:
            break;
        default:
            throw new RuntimeException( "Unknown value type " + getValueType() );
        }
    }

    protected void writeIndexCommandHeader( WritableChannel channel ) throws IOException
    {
        writeIndexCommandHeader( channel, getValueType(), getEntityType(), needsLong( getEntityId() ),
                startNodeNeedsLong(), endNodeNeedsLong(), getIndexNameId(), getKeyId() );
    }

    protected static void writeIndexCommandHeader( WritableChannel channel, byte valueType, byte entityType,
            byte entityIdNeedsLong, byte startNodeNeedsLong, byte endNodeNeedsLong, int indexNameId, int keyId )
                    throws IOException
    {
        channel.put( (byte) ((valueType << 2) | (entityType << 1) | entityIdNeedsLong) );
        channel.put( (byte) ((startNodeNeedsLong << 7) | (endNodeNeedsLong << 6)) );
        channel.putShort( (short) indexNameId );
        channel.putShort( (short) keyId );
    }

    protected void putIntOrLong( WritableChannel channel, long id ) throws IOException
    {
        if ( needsLong( id ) == 1 )
        {
            channel.putLong( id );
        }
        else
        {
            channel.putInt( (int) id );
        }
    }

    public static class AddNodeCommand extends IndexCommand
    {
        public void init( int indexNameId, long entityId, int keyId, Object value )
        {
            super.init( NeoCommandType.INDEX_ADD_COMMAND, indexNameId, IndexEntityType.Node.id(),
                    entityId, keyId, value );
        }

        @Override
        public boolean handle( CommandVisitor visitor ) throws IOException
        {
            return visitor.visitIndexAddNodeCommand( this );
        }

        @Override
        public String toString()
        {
            return "AddNode[index:" + indexNameId + ", id:" + entityId + ", key:" + keyId + ", value:" + value + "]";
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.INDEX_ADD_COMMAND );
            writeToFile( channel );
        }
    }

    protected static byte needsLong( long value )
    {
        return value > Integer.MAX_VALUE ? (byte)1 : (byte)0;
    }

    public static class AddRelationshipCommand extends IndexCommand
    {
        private long startNode;
        private long endNode;

        public void init( int indexNameId, long entityId, int keyId,
                Object value, long startNode, long endNode )
        {
            super.init( NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND, indexNameId, IndexEntityType.Relationship.id(),
                    entityId, keyId, value );
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
        public byte startNodeNeedsLong()
        {
            return needsLong( startNode );
        }

        @Override
        public byte endNodeNeedsLong()
        {
            return needsLong( endNode );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }
            AddRelationshipCommand that = (AddRelationshipCommand) o;
            return startNode == that.startNode && endNode == that.endNode;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( super.hashCode(), startNode, endNode );
        }

        @Override
        public boolean handle( CommandVisitor visitor ) throws IOException
        {
            return visitor.visitIndexAddRelationshipCommand( this );
        }

        @Override
        public String toString()
        {
            return "AddRelationship[index:" + indexNameId + ", id:" + entityId + ", key:" + keyId +
                    ", value:" + value + "(" + (value != null ? value.getClass().getSimpleName() : "null") + ")" +
                    ", startNode:" + startNode +
                    ", endNode:" + endNode +
                    "]";
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND );
            writeToFile( channel );
            putIntOrLong( channel, getStartNode() );
            putIntOrLong( channel, getEndNode() );
        }
    }

    public static class RemoveCommand extends IndexCommand
    {
        public void init( int indexNameId, byte entityType, long entityId, int keyId, Object value )
        {
            super.init( NeoCommandType.INDEX_REMOVE_COMMAND, indexNameId, entityType, entityId, keyId, value );
        }

        @Override
        public boolean handle( CommandVisitor visitor ) throws IOException
        {
            return visitor.visitIndexRemoveCommand( this );
        }

        @Override
        public String toString()
        {
            return format( "Remove%s[index:%d, id:%d, key:%d, value:%s]",
                    IndexEntityType.byId( entityType ).nameToLowerCase(), indexNameId, entityId, keyId, value );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.INDEX_REMOVE_COMMAND );
            writeToFile( channel );
        }
    }

    public static class DeleteCommand extends IndexCommand
    {
        public void init( int indexNameId, byte entityType )
        {
            super.init( NeoCommandType.INDEX_DELETE_COMMAND, indexNameId, entityType, 0L, (byte)0, null );
        }

        @Override
        public boolean handle( CommandVisitor visitor ) throws IOException
        {
            return visitor.visitIndexDeleteCommand( this );
        }

        @Override
        public String toString()
        {
            return "Delete[index:" + indexNameId + ", type:" + IndexEntityType.byId( entityType ).nameToLowerCase() + "]";
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.INDEX_DELETE_COMMAND );
            writeIndexCommandHeader( channel );
        }
    }

    public static class CreateCommand extends IndexCommand
    {
        private Map<String, String> config;

        public void init( int indexNameId, byte entityType, Map<String, String> config )
        {
            super.init( NeoCommandType.INDEX_CREATE_COMMAND, indexNameId, entityType, 0L, (byte)0, null );
            this.config = config;
        }

        public Map<String, String> getConfig()
        {
            return config;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }
            CreateCommand that = (CreateCommand) o;
            return Objects.equals( config, that.config );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( super.hashCode(), config );
        }

        @Override
        public boolean handle( CommandVisitor visitor ) throws IOException
        {
            return visitor.visitIndexCreateCommand( this );
        }

        @Override
        public String toString()
        {
            return format( "Create%sIndex[index:%d, config:%s]",
                    IndexEntityType.byId( entityType ).nameToLowerCase(), indexNameId, config );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.INDEX_CREATE_COMMAND );
            writeIndexCommandHeader( channel );
            channel.putShort( (short) getConfig().size() );
            for ( Map.Entry<String,String> entry : getConfig().entrySet() )
            {
                write2bLengthAndString( channel, entry.getKey() );
                write2bLengthAndString( channel, entry.getValue() );
            }
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        IndexCommand that = (IndexCommand) o;
        return commandType == that.commandType &&
               indexNameId == that.indexNameId &&
               entityType == that.entityType &&
               entityId == that.entityId &&
               keyId == that.keyId &&
               valueType == that.valueType &&
               Objects.equals( value, that.value );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), commandType, indexNameId, entityType, entityId, keyId, valueType, value );
    }

    public byte getCommandType()
    {
        return commandType;
    }

    public byte getValueType()
    {
        return valueType;
    }
}
