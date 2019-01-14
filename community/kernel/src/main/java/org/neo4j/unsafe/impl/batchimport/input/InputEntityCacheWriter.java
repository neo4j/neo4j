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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;

import static java.lang.Integer.max;
import static org.neo4j.helpers.Numbers.safeCastLongToShort;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_ENTITIES;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_HEADER;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.GROUP_TOKEN;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_FIRST_PROPERTY_ID;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HIGH_TOKEN_TYPE;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_TOKEN;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.NEW_GROUP;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.PROPERTY_KEY_TOKEN;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.SAME_GROUP;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.newChunkHeaderBuffer;

/**
 * Abstract class for caching entities or derivative to disk using a binary format.
 * Currently each token type is limited to have a maximum of {#link Integer.MAX_VALUE} items.
 *
 * This instance provides means of wrapping {@link InputEntityVisitor} so that they automatically cache what they see.
 * Each thread that visits data {@link #wrap(InputEntityVisitor)} its own visitor and serializes data into
 * its thread-local buffer. When full it {@link #writeChunk(ByteBuffer) writes} that chunk of data to the {@link #channel},
 * clearing its buffer and ready to serialize more data into it.
 */
abstract class InputEntityCacheWriter implements InputCacher
{
    static final String[] EMPTY_STRING_ARRAY = new String[0];

    protected final StoreChannel channel;
    private final ByteBuffer chunkHeaderChannel = newChunkHeaderBuffer();

    private final FlushableChannel header;
    private final int chunkSize;

    private final int[] nextKeyId = new int[HIGH_TOKEN_TYPE];
    private final int[] maxKeyId = new int[HIGH_TOKEN_TYPE];

    @SuppressWarnings( "unchecked" )
    private final Map<String,Integer>[] tokens = new Map[HIGH_TOKEN_TYPE];

    protected InputEntityCacheWriter( StoreChannel channel, StoreChannel header, RecordFormats recordFormats, int chunkSize )
    {
        this.chunkSize = chunkSize;
        initMaxTokenKeyIds( recordFormats );

        // We don't really care about versions, it's just that apart from that the WritableLogChannel
        // does precisely what we want and there's certainly value in not duplicating that functionality.
        this.channel = channel;
        this.header = new PhysicalFlushableChannel( header );
        for ( int i = 0; i < tokens.length; i++ )
        {
            tokens[i] = new ConcurrentHashMap<>();
        }
    }

    @Override
    public final synchronized InputEntityVisitor wrap( InputEntityVisitor visitor )
    {
        return instantiateWrapper( visitor, chunkSize );
    }

    protected abstract SerializingInputEntityVisitor instantiateWrapper( InputEntityVisitor visitor, int chunkSize );

    void writeChunk( ByteBuffer buffer ) throws IOException
    {
        // reserve space for the chunk
        long dataStartPosition;
        synchronized ( this )
        {
            // write header
            int chunkLength = buffer.limit();
            chunkHeaderChannel.clear();
            chunkHeaderChannel.putInt( chunkLength );
            chunkHeaderChannel.flip();
            channel.writeAll( chunkHeaderChannel );

            dataStartPosition = channel.position();
            channel.position( dataStartPosition + chunkLength );
        }

        // write chunk data
        channel.writeAll( buffer, dataStartPosition );
    }

    @Override
    public void close() throws IOException
    {
        // write end tokens in the channels
        header.put( END_OF_HEADER );
        writeChunk( ByteBuffer.wrap( new byte[0] ) );

        channel.close();
        header.close();
    }

    private void initMaxTokenKeyIds( RecordFormats recordFormats )
    {
        maxKeyId[PROPERTY_KEY_TOKEN] = getMaxAcceptableTokenId( recordFormats.propertyKeyToken().getMaxId() );
        maxKeyId[LABEL_TOKEN] = getMaxAcceptableTokenId( recordFormats.labelToken().getMaxId() );
        maxKeyId[RELATIONSHIP_TYPE_TOKEN] = getMaxAcceptableTokenId( recordFormats.relationshipTypeToken().getMaxId() );
        maxKeyId[GROUP_TOKEN] = getMaxAcceptableTokenId( recordFormats.relationshipGroup().getMaxId() );
    }

    private static int getMaxAcceptableTokenId( long maxId )
    {
        return (int) Math.min( Integer.MAX_VALUE, maxId );
    }

    private int getOrCreateToken( byte type, String key ) throws IOException
    {
        Integer id = tokens[type].get( key );
        if ( id == null )
        {
            synchronized ( header )
            {
                id = tokens[type].get( key );
                if ( id == null )
                {
                    if ( nextKeyId[type] == maxKeyId[type] )
                    {
                        throw new UnsupportedOperationException( "Too many tokens. Creation of more then " +
                                maxKeyId[type] + " tokens is not supported." );
                    }
                    tokens[type].put( key, id = nextKeyId[type]++ );
                    header.put( type );
                    ValueType.stringType().write( key, header );
                }
            }
        }
        return id;
    }

    abstract class SerializingInputEntityVisitor extends InputEntity
    {
        private final int lengthThreshold;
        private byte[] array;
        protected ByteBuffer buffer;
        private FlushableChannel bufferAsChannel;
        private final int[] previousGroupIds = new int[2];

        SerializingInputEntityVisitor( InputEntityVisitor actual, int chunkSize )
        {
            super( actual );
            this.lengthThreshold = chunkSize;
            this.array = new byte[chunkSize + chunkSize / 10]; // some wiggle room
            this.buffer = ByteBuffer.wrap( array );
            this.bufferAsChannel = new ByteBufferFlushableChannel( buffer );
        }

        @Override
        public void endOfEntity() throws IOException
        {
            super.endOfEntity();

            // serialize into the buffer
            serializeEntity();
            if ( buffer.position() >= lengthThreshold )
            {
                flushChunk();
                clearState();
            }
        }

        protected void clearState()
        {
            Arrays.fill( previousGroupIds, Group.GLOBAL.id() );
        }

        protected abstract void serializeEntity() throws IOException;

        protected void writeProperties() throws IOException
        {
            if ( hasPropertyId )
            {
                buffer( Short.BYTES + Long.BYTES ).putShort( HAS_FIRST_PROPERTY_ID ).putLong( propertyId );
            }
            else
            {
                Object[] properties = properties();
                buffer( Short.BYTES ).putShort( safeCastLongToShort( properties.length / 2 ) );
                for ( int i = 0; i < properties.length; i++ )
                {
                    Object key = properties[i++];
                    Object value = properties[i];
                    if ( value == null )
                    {
                        continue;
                    }
                    writeToken( PROPERTY_KEY_TOKEN, key );
                    writeValue( value );
                }
            }
        }

        protected ByteBuffer buffer( int requiredSpace )
        {
            int position = buffer.position();
            if ( position + requiredSpace >= buffer.capacity() )
            {
                array = Arrays.copyOf( array, max( array.length * 2, position + requiredSpace ) ); // at least double in size
                buffer = ByteBuffer.wrap( array );
                buffer.position( position );
                bufferAsChannel = new ByteBufferFlushableChannel( buffer );
            }
            return buffer;
        }

        protected void writeGroup( Group group, int slot ) throws IOException
        {
            group = group != null ? group : Group.GLOBAL;
            if ( group.id() == previousGroupIds[slot] )
            {
                buffer( Byte.BYTES ).put( SAME_GROUP );
            }
            else
            {
                buffer( Byte.BYTES + Integer.BYTES ).put( NEW_GROUP ).putInt( previousGroupIds[slot] = group.id() );
                writeToken( GROUP_TOKEN, group.name() );
            }
        }

        protected void writeValue( Object value ) throws IOException
        {
            ValueType type = ValueType.typeOf( value );
            int length = type.length( value );
            buffer( Byte.BYTES + length ).put( type.id() );
            type.write( value, bufferAsChannel );
        }

        protected void writeToken( byte type, Object key ) throws IOException
        {
            if ( key instanceof String )
            {
                int id = getOrCreateToken( type, (String) key );
                buffer( Integer.BYTES ).putInt( id );
            }
            else if ( key instanceof Integer )
            {
                // Here we signal that we have a real token id, not to be confused by the local and contrived
                // token ids we generate in here. Following this -1 is the real token id.
                buffer( Integer.BYTES + Integer.BYTES ).putInt( (short) -1 ).putInt( (Integer) key );
            }
            else
            {
                throw new IllegalArgumentException( "Invalid key " + key + ", " + key.getClass() );
            }
        }

        @Override
        public void close() throws IOException
        {
            if ( buffer.position() > 0 )
            {
                flushChunk();
            }
        }

        private void flushChunk() throws IOException
        {
            buffer( Short.BYTES ).putShort( END_OF_ENTITIES );
            buffer.flip();
            writeChunk( buffer );
            buffer.clear();
        }
    }
}
