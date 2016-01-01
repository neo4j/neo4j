/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriter;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyLogIoUtil;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

public class LegacyLuceneCommandReader
{

    public static LegacyLogIoUtil.CommandReader newReader()
    {
        return new LegacyLogIoUtil.CommandReader()
        {
            @Override
            public XaCommand readCommand( ReadableByteChannel channel, ByteBuffer buffer ) throws IOException
            {
                return LuceneCommand.readCommand( channel, buffer );
            }
        };
    }

    public static XaCommandWriter newWriter()
    {
        return new XaCommandWriter()
        {
            @Override
            public void write( XaCommand command, LogBuffer buffer ) throws IOException
            {
                ((LuceneCommand) command).writeToFile( buffer );
            }
        };
    }

    /**
     * This class is partial copy-paste from org.neo4j.index.impl.lucene.IndexIdentifier
     * to avoid dependency of 'kernel' module on 'lucene-index' module.
     */
    private static class IndexIdentifier
    {
        final String indexName;
        final byte entityTypeByte;

        public IndexIdentifier( byte entityTypeByte, String indexName )
        {
            this.entityTypeByte = entityTypeByte;
            this.indexName = indexName;
        }
    }

    /**
     * This class is partial copy-paste from org.neo4j.index.impl.lucene.RelationshipId
     * to avoid dependency of 'kernel' module on 'lucene-index' module.
     */
    private static class RelationshipId
    {
        final long id;
        final long startNode;
        final long endNode;

        RelationshipId( long id, long startNode, long endNode )
        {
            this.id = id;
            this.startNode = startNode;
            this.endNode = endNode;
        }
    }

    /**
     * This class is partial copy-paste from org.neo4j.index.impl.lucene.LuceneCommand
     * to avoid dependency of 'kernel' module on 'lucene-index' module.
     */
    private static abstract class LuceneCommand extends XaCommand
    {
        private static final byte ADD_COMMAND = (byte) 1;
        private static final byte REMOVE_COMMAND = (byte) 2;
        private static final byte DELETE_COMMAND = (byte) 3;
        private static final byte CREATE_INDEX_COMMAND = (byte) 4;

        public static final byte NODE = (byte) 1;
        public static final byte RELATIONSHIP = (byte) 2;

        private static final byte VALUE_TYPE_NULL = (byte) 0;
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
        final byte entityType;

        private LuceneCommand( IndexIdentifier indexId, byte entityType, Object entityId, String key, Object value, byte type )
        {
            assert entityType == NODE || entityType == RELATIONSHIP;
            this.indexId = indexId;
            this.entityType = entityType;
            this.entityId = entityId;
            this.key = key;
            this.value = value;
            this.type = type;
        }

        protected void writeToFile( LogBuffer buffer ) throws IOException
        {
            buffer.put( type );
            buffer.put( entityType );
            char[] indexName = indexId.indexName.toCharArray();
            buffer.putInt( indexName.length );
            long id = entityId instanceof Long ? (Long) entityId : ((RelationshipId) entityId).id;
            buffer.putLong( id );
            char[] key = this.key == null ? null : this.key.toCharArray();
            buffer.putInt( key == null ? -1 : key.length );

            byte valueType;
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
            if ( key != null )
            {
                buffer.put( key );
            }
            if ( valueType == VALUE_TYPE_STRING )
            {
                char[] charValue = value.toString().toCharArray();
                buffer.putInt( charValue.length );
                buffer.put( charValue );
            }
            else if ( valueType != VALUE_TYPE_NULL )
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

        private static class AddCommand extends LuceneCommand
        {
            AddCommand( IndexIdentifier indexId, byte entityType, Object entityId, String key, Object value )
            {
                super( indexId, entityType, entityId, key, value, ADD_COMMAND );
            }
        }

        private static class AddRelationshipCommand extends LuceneCommand
        {
            AddRelationshipCommand( IndexIdentifier indexId, byte entityType, RelationshipId entityId, String key,
                                    Object value )
            {
                super( indexId, entityType, entityId, key, value, ADD_COMMAND );
            }

            @Override
            protected void writeToFile( LogBuffer buffer ) throws IOException
            {
                super.writeToFile( buffer );
                buffer.putLong( ((RelationshipId) entityId).startNode );
                buffer.putLong( ((RelationshipId) entityId).endNode );
            }
        }

        private static class RemoveCommand extends LuceneCommand
        {
            RemoveCommand( IndexIdentifier indexId, byte entityType, Object entityId, String key, Object value )
            {
                super( indexId, entityType, entityId, key, value, REMOVE_COMMAND );
            }
        }

        private static class DeleteCommand extends LuceneCommand
        {
            DeleteCommand( IndexIdentifier indexId )
            {
                super( indexId, indexId.entityTypeByte, -1L, "", "", DELETE_COMMAND );
            }

        }

        private static class CreateIndexCommand extends LuceneCommand
        {
            static final IndexIdentifier FAKE_IDENTIFIER = new IndexIdentifier( (byte) 9, "create index" );
            private final String name;
            private final Map<String, String> config;

            CreateIndexCommand( byte entityType, String name, Map<String, String> config )
            {
                super( FAKE_IDENTIFIER, entityType, -1L, null, null, CREATE_INDEX_COMMAND );
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
            protected void writeToFile( LogBuffer buffer ) throws IOException
            {
                buffer.put( type );
                buffer.put( entityType );
                writeLengthAndString( buffer, name );
                buffer.putInt( config.size() );
                for ( Map.Entry<String, String> entry : config.entrySet() )
                {
                    writeLengthAndString( buffer, entry.getKey() );
                    writeLengthAndString( buffer, entry.getValue() );
                }
            }

            private static void writeLengthAndString( LogBuffer buffer, String string ) throws IOException
            {
                char[] chars = string.toCharArray();
                buffer.putInt( chars.length );
                buffer.put( chars );
            }
        }

        private static XaCommand readCommand( ReadableByteChannel channel,
                                      ByteBuffer buffer/*, LuceneDataSource dataSource*/ ) throws IOException
        {
            // Read what type of command it is
            buffer.clear();
            buffer.limit( 2 );
            if ( channel.read( buffer ) != buffer.limit() )
            {
                return null;
            }
            buffer.flip();
            byte commandType = buffer.get();
            byte entityTypeByte = buffer.get();

            if ( commandType == CREATE_INDEX_COMMAND )
            {
                buffer.clear();
                String name = IoPrimitiveUtils.readLengthAndString( channel, buffer );
                if ( name == null )
                {
                    return null;
                }
                int size = IoPrimitiveUtils.readInt( channel, buffer );
                Map<String, String> config = new HashMap<>();
                for ( int i = 0; i < size; i++ )
                {
                    String key = IoPrimitiveUtils.readLengthAndString( channel, buffer );
                    String value = IoPrimitiveUtils.readLengthAndString( channel, buffer );
                    if ( key == null || value == null )
                    {
                        return null;
                    }
                    config.put( key, value );
                }
                return new CreateIndexCommand( entityTypeByte, name, config );
            }
            else
            {
                // Read the command data
                buffer.clear();
                buffer.limit( 17 );
                if ( channel.read( buffer ) != buffer.limit() )
                {
                    return null;
                }
                buffer.flip();


                int indexNameLength = buffer.getInt();
                long entityId = buffer.getLong();
                int keyCharLength = buffer.getInt();
                byte valueType = buffer.get();

                String indexName = IoPrimitiveUtils.readString( channel, buffer, indexNameLength );
                if ( indexName == null )
                {
                    return null;
                }

                String key = null;
                if ( keyCharLength != -1 )
                {
                    key = IoPrimitiveUtils.readString( channel, buffer, keyCharLength );
                    if ( key == null )
                    {
                        return null;
                    }
                }

                Object value = null;
                if ( valueType >= VALUE_TYPE_INT && valueType <= VALUE_TYPE_DOUBLE )
                {
                    switch ( valueType )
                    {
                        case VALUE_TYPE_INT:
                            value = IoPrimitiveUtils.readInt( channel, buffer );
                            break;
                        case VALUE_TYPE_LONG:
                            value = IoPrimitiveUtils.readLong( channel, buffer );
                            break;
                        case VALUE_TYPE_FLOAT:
                            value = IoPrimitiveUtils.readFloat( channel, buffer );
                            break;
                        case VALUE_TYPE_DOUBLE:
                            value = IoPrimitiveUtils.readDouble( channel, buffer );
                            break;
                    }
                }
                else if ( valueType == VALUE_TYPE_STRING )
                {
                    value = IoPrimitiveUtils.readLengthAndString( channel, buffer );
                }
                if ( valueType != VALUE_TYPE_NULL && value == null )
                {
                    return null;
                }

                Long startNodeId = null;
                Long endNodeId = null;
                if ( commandType == ADD_COMMAND && entityTypeByte == RELATIONSHIP )
                {
                    startNodeId = IoPrimitiveUtils.readLong( channel, buffer );
                    endNodeId = IoPrimitiveUtils.readLong( channel, buffer );
                    if ( startNodeId == null || endNodeId == null )
                    {
                        return null;
                    }
                }

                IndexIdentifier identifier = new IndexIdentifier( entityTypeByte, indexName );

                switch ( commandType )
                {
                    case ADD_COMMAND:
                        return entityTypeByte == NODE ?
                                new AddCommand( identifier, entityTypeByte, entityId, key, value ) :
                                new AddRelationshipCommand(
                                        identifier,
                                        entityTypeByte,
                                        new RelationshipId( entityId, startNodeId, endNodeId ),
                                        key,
                                        value
                                );
                    case REMOVE_COMMAND:
                        return new RemoveCommand( identifier, entityTypeByte, entityId, key, value );
                    case DELETE_COMMAND:
                        return new DeleteCommand( identifier );
                    default:
                        throw new IOException( "Unknown command type[" + commandType + "]" );
                }
            }
        }
    }
}
