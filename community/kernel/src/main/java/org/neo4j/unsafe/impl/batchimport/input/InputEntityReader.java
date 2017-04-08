/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.util.collection.ContinuableArrayCursor;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.staging.TicketedProcessing;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToInt;
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

/**
 * Abstract class for reading cached entities previously stored using {@link InputEntityCacher} or derivative.
 * Entity data is read in batches, each handed off to one ore more processors which interprets the bytes
 * into {@link InputEntity} instances. From the outside this is simply an {@link InputIterator},
 * the parallelization happens inside.
 */
abstract class InputEntityReader<ENTITY extends InputEntity> extends InputIterator.Adapter<ENTITY>
{
    // Used by BatchProvidingIterator. To feed jobs into TicketedProcessing
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private int lineNumber;
    private TicketedProcessing<byte[],Void,Object[]> processing;

    // Used by workers, immutable
    private final PrimitiveIntObjectMap<String>[] tokens;

    // Not used by workers
    private final Runnable closeAction;
    private final ReadAheadLogChannel cacheChannel;
    private final ContinuableArrayCursor<Object> processedEntities;

    protected static class ProcessorState
    {
        // Used by workers, mutable
        protected final Group[] previousGroups;
        protected String previousType;
        protected String[] previousLabels = InputEntity.NO_LABELS;
        protected ReadableClosablePositionAwareChannel batchChannel;

        public ProcessorState( byte[] batchData )
        {
            this.batchChannel = new InMemoryClosableChannel( batchData, true/*append*/ );
            this.previousGroups = new Group[2];
            for ( int i = 0; i < previousGroups.length; i++ )
            {
                previousGroups[i] = Group.GLOBAL;
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    InputEntityReader( StoreChannel channel, StoreChannel header, int bufferSize, Runnable closeAction,
            int maxNbrOfProcessors )
            throws IOException
    {
        tokens = new PrimitiveIntObjectMap[HIGH_TOKEN_TYPE];
        tokens[PROPERTY_KEY_TOKEN] = Primitive.intObjectMap();
        tokens[LABEL_TOKEN] = Primitive.intObjectMap();
        tokens[RELATIONSHIP_TYPE_TOKEN] = Primitive.intObjectMap();
        tokens[GROUP_TOKEN] = Primitive.intObjectMap();
        cacheChannel = reader( channel, bufferSize );
        this.closeAction = closeAction;
        readHeader( header );

        /** The processor is the guy converting the byte[] to ENTITY[]
         *  we will have a lot of those guys
         */
        BiFunction<byte[],Void,Object[]> processor = (batchData,ignore) ->
        {
            ProcessorState state = new ProcessorState( batchData );
            try
            {
                int nbrOfEntries = state.batchChannel.getInt();

                // Read all Entities and put in ENTITY[] to return.
                Object[] result = new Object[nbrOfEntries];
                for ( int i = 0; i < nbrOfEntries; i++ )
                {
                    result[i] = readOneEntity( state );
                }

                return result;
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( e );
            }
        };
        Supplier<Void> noState = () -> null;
        processing = new TicketedProcessing<>( getClass().getName(), maxNbrOfProcessors, processor, noState );

        // This iterator is only called from TicketedProcessing.slurp that submit jobs to new threads.
        Iterator<byte[]> iterator = new BatchProvidingIterator();
        processing.slurp( iterator, true );

        processedEntities = new ContinuableArrayCursor<>( () -> processing.next() );
    }

    private ReadAheadLogChannel reader( StoreChannel channel, int bufferSize ) throws IOException
    {
        return new ReadAheadLogChannel(
                new PhysicalLogVersionedStoreChannel( channel, 0, (byte) 0 ), NO_MORE_CHANNELS, bufferSize );
    }

    private void readHeader( StoreChannel header ) throws IOException
    {
        try ( ReadableClosableChannel reader = reader( header, (int) ByteUnit.kibiBytes( 8 ) ) )
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

    protected final ENTITY readOneEntity( ProcessorState state )
    {
        ReadableClosablePositionAwareChannel channel = state.batchChannel;
        try
        {
            // Read next entity
            Object properties = readProperties( channel );
            if ( properties == null )
            {
                return null;
            }

            return readNextOrNull( properties, state );
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't read cached node data", e );
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    protected ENTITY fetchNextOrNull()
    {
        return processedEntities.next() ? (ENTITY) processedEntities.get() : null;
    }

    protected abstract ENTITY readNextOrNull( Object properties, ProcessorState state ) throws IOException;

    private Object readProperties( ReadableClosablePositionAwareChannel channel ) throws IOException
    {
        short count = channel.getShort();
        switch ( count )
        {
        // This is a special value denoting the end of the stream. This is done like this since
        // properties are the first thing read for every entity.
        case END_OF_ENTITIES: return null;
        case HAS_FIRST_PROPERTY_ID: return channel.getLong();
        case 0: return InputEntity.NO_PROPERTIES;
        default:
            Object[] properties = new Object[count * 2];
            for ( int i = 0; i < properties.length; i++ )
            {
                properties[i++] = readToken( PROPERTY_KEY_TOKEN, channel );
                properties[i] = readValue( channel );
            }
            return properties;
        }
    }

    protected Object readToken( byte type, ReadableClosablePositionAwareChannel channel ) throws IOException
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

    protected Object readValue( ReadableClosablePositionAwareChannel channel ) throws IOException
    {
        return ValueType.typeOf( channel.get() ).read( channel );
    }

    protected Group readGroup( int slot, ProcessorState state ) throws IOException
    {
        ReadableClosablePositionAwareChannel channel = state.batchChannel;
        byte groupMode = channel.get();
        switch ( groupMode )
        {
        case SAME_GROUP: return state.previousGroups[slot];
        case NEW_GROUP: return state.previousGroups[slot] = new Group.Adapter( channel.getInt(),
                (String) readToken( GROUP_TOKEN, channel ) );
        default: throw new IllegalArgumentException( "Unknown group mode " + groupMode );
        }
    }

    @Override
    public String sourceDescription()
    {
        return "cache"; // it's OK we shouldn't need these things the second time around
    }

    @Override
    public long lineNumber()
    {
        return lineNumber;
    }

    @Override
    public long position()
    {
        try
        {
            return cacheChannel.getCurrentPosition( positionMarker ).getByteOffset();
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't get position from cached input data", e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            processing.close();
            cacheChannel.close();
            closeAction.run();
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't close channel for cached input data", e );
        }
    }

    @Override
    public int processors( int delta )
    {
        return processing.processors( delta );
    }

    private class BatchProvidingIterator extends PrefetchingIterator<byte[]>
    {
        @Override
        protected byte[] fetchNextOrNull()
        {
            try
            {
                int batchSize = safeCastLongToInt( cacheChannel.getLong() );
                if ( batchSize == InputCache.END_OF_CACHE )
                {
                    // We have reached end of cache
                    return null;
                }
                byte[] bytes = new byte[batchSize];
                cacheChannel.get( bytes, batchSize );

                return bytes;
            }
            catch ( IOException e )
            {
                // Batch size was probably wrong if we ended up here.
                throw new RuntimeException( e );
            }
        }
    }
}
