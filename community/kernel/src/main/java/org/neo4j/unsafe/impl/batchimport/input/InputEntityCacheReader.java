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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

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
 * Abstract class for reading cached entities previously stored using {@link InputEntityCacheWriter} or derivative.
 * Entity data is read in batches, each handed off to one ore more processors which interprets the bytes
 * into input data. From the outside this is simply an {@link InputIterator},
 * the parallelization happens inside.
 */
abstract class InputEntityCacheReader implements InputIterator
{
    // Used by workers, immutable
    private final PrimitiveIntObjectMap<String>[] tokens;

    // Not used by workers
    private final StoreChannel channel;
    private final ByteBuffer chunkHeaderBuffer = newChunkHeaderBuffer();
    private boolean end;

    @SuppressWarnings( "unchecked" )
    InputEntityCacheReader( StoreChannel channel, StoreChannel header )
            throws IOException
    {
        tokens = new PrimitiveIntObjectMap[HIGH_TOKEN_TYPE];
        tokens[PROPERTY_KEY_TOKEN] = Primitive.intObjectMap();
        tokens[LABEL_TOKEN] = Primitive.intObjectMap();
        tokens[RELATIONSHIP_TYPE_TOKEN] = Primitive.intObjectMap();
        tokens[GROUP_TOKEN] = Primitive.intObjectMap();
        this.channel = channel;
        readHeader( header );
    }

    @Override
    public boolean next( InputChunk chunk ) throws IOException
    {
        InputEntityDeserializer realChunk = (InputEntityDeserializer) chunk;

        long dataStartPosition;
        int length;
        synchronized ( channel )
        {
            if ( end )
            {
                return false;
            }

            chunkHeaderBuffer.clear();
            channel.read( chunkHeaderBuffer );
            chunkHeaderBuffer.flip();
            length = chunkHeaderBuffer.getInt();
            dataStartPosition = channel.position();
            channel.position( dataStartPosition + length );
            if ( length == 0 )
            {
                end = true;
                return false;
            }
        }

        realChunk.initialize( dataStartPosition, length );
        return true;
    }

    private static ReadAheadChannel<StoreChannel> reader( StoreChannel channel )
    {
        return new ReadAheadChannel<>( channel );
    }

    private void readHeader( StoreChannel header ) throws IOException
    {
        try ( ReadableClosableChannel reader = reader( header ) )
        {
            int[] tokenIds = new int[HIGH_TOKEN_TYPE];
            byte type;
            while ( (type = reader.get()) != END_OF_HEADER )
            {
                int tokenId = tokenIds[type]++;
                String name = (String) ValueType.stringType().read( reader );
                tokens[type].put( tokenId, name );
            }
        }
    }

    @Override
    public void close()
    {
        try
        {
            channel.close();
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't close channel for cached input data", e );
        }
    }

    abstract class InputEntityDeserializer implements InputChunk
    {
        private ByteBuffer buffer;
        protected ReadableClosableChannel channel;
        protected Group[] previousGroups = new Group[2];

        void initialize( long startPosition, int chunkLength ) throws IOException
        {
            if ( buffer == null || buffer.capacity() < chunkLength )
            {
                buffer = ByteBuffer.allocate( chunkLength + chunkLength / 10 );
                channel = new ByteBufferReadableChannel( buffer );
            }
            buffer.clear();
            buffer.limit( chunkLength );
            InputEntityCacheReader.this.channel.read( buffer, startPosition );
            buffer.flip();
            clearState();
        }

        protected void clearState()
        {
            Arrays.fill( previousGroups, Group.GLOBAL );
        }

        @Override
        public void close()
        {
        }

        protected boolean readProperties( InputEntityVisitor visitor ) throws IOException
        {
            short count = channel.getShort();
            if ( count == END_OF_ENTITIES )
            {
                // This is a special value denoting the end of the stream. This is done like this since
                // properties are the first thing read for every entity.
                return false;
            }
            else if ( count == HAS_FIRST_PROPERTY_ID )
            {
                visitor.propertyId( channel.getLong() );
            }
            else
            {
                for ( int i = 0; i < count; i++ )
                {
                    Object token = readToken( PROPERTY_KEY_TOKEN );
                    Object value = readValue( );
                    if ( token instanceof Integer )
                    {
                        visitor.property( (Integer) token, value );
                    }
                    else
                    {
                        visitor.property( (String) token, value );
                    }
                }
            }
            return true;
        }

        protected Object readToken( byte type ) throws IOException
        {
            int id = channel.getInt();
            if ( id == -1 )
            {
                // This is a real token id
                return channel.getInt();
            }

            String name = tokens[type].get( id );
            if ( name == null )
            {
                throw new IllegalArgumentException( "Unknown token " + id );
            }
            return name;
        }

        protected Object readValue() throws IOException
        {
            return ValueType.typeOf( channel.get() ).read( channel );
        }

        protected Group readGroup( int slot ) throws IOException
        {
            byte groupMode = channel.get();
            switch ( groupMode )
            {
            case SAME_GROUP: return previousGroups[slot];
            case NEW_GROUP: return previousGroups[slot] = new Group.Adapter( channel.getInt(),
                    (String) readToken( GROUP_TOKEN ) );
            default: throw new IllegalArgumentException( "Unknown group mode " + groupMode );
            }
        }
    }
}
