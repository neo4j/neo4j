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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PositionAwarePhysicalFlushableChannel;

import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToShort;
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
 * Abstract class for caching {@link InputEntity} or derivative to disk using a binary format.
 * Currently each token type limited to have as maximum {#link Integer.MAX_VALUE} items.
 */
abstract class InputEntityCacher<ENTITY extends InputEntity> implements Receiver<ENTITY[],IOException>
{
    protected final PositionAwarePhysicalFlushableChannel channel;
    private final FlushableChannel header;
    private final StoreChannel storeChannel;
    private final StoreChannel headerChannel;
    private final int[] previousGroupIds;

    private final int[] nextKeyId = new int[HIGH_TOKEN_TYPE];
    private final int[] maxKeyId = new int[HIGH_TOKEN_TYPE];

    @SuppressWarnings( "unchecked" )
    private final Map<String,Integer>[] tokens = new Map[HIGH_TOKEN_TYPE];

    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private LogPosition currentBatchStartPosition;
    private int entitiesWritten;
    private final int batchSize;

    protected InputEntityCacher( StoreChannel channel, StoreChannel header, RecordFormats recordFormats,
            int bufferSize, int batchSize, int groupSlots )
            throws IOException
    {
        this.storeChannel = channel;
        this.headerChannel = header;
        this.batchSize = batchSize;
        this.previousGroupIds = new int[groupSlots];

        initMaxTokenKeyIds( recordFormats );
        clearState();

        // We don't really care about versions, it's just that apart from that the WritableLogChannel
        // does precisely what we want and there's certainly value in not duplicating that functionality.
        this.channel = new PositionAwarePhysicalFlushableChannel(
                new PhysicalLogVersionedStoreChannel( channel, 0, (byte)0 ), bufferSize );
        this.header = new PositionAwarePhysicalFlushableChannel(
                new PhysicalLogVersionedStoreChannel( header, 0, (byte)0 ), (int) ByteUnit.kibiBytes( 8 ) );
        for ( int i = 0; i < tokens.length; i++ )
        {
            tokens[i] = new HashMap<>();
        }
    }

    @Override
    public void receive( ENTITY[] batch ) throws IOException
    {
        for ( ENTITY entity : batch )
        {
            if ( entitiesWritten % batchSize == 0 )
            {
                newBatch();
            }
            entitiesWritten++;
            writeEntity( entity );
        }
    }

    // [ A  ][ B  ][.................................]
    //             |<-----A------------------------->| (B entities in total)
    // |<------------------------------------------->|
    private void newBatch() throws IOException
    {
        channel.getCurrentPosition( positionMarker );

        // Set byte size in previous batch
        if ( entitiesWritten > 0 )
        {
            // Remember the current position
            // Go back to the start of this batch
            channel.setCurrentPosition( currentBatchStartPosition );
            // and set the size in that long field (not counting the size of the size field)
            channel.putLong( positionMarker.getByteOffset() - currentBatchStartPosition.getByteOffset() - Long.BYTES );
            // and number of entities written
            channel.putInt( entitiesWritten );
            // Now go back to where we were before updating this size field
            channel.setCurrentPosition( positionMarker.newPosition() );
        }

        // Always add mark for the new batch here, this will simplify reader logic
        startBatch();
    }

    private void startBatch() throws IOException
    {
        // Make room for size in new batch and number of entities
        // Until this batch is finished, this mark the end of the cache.
        clearState();
        entitiesWritten = 0;
        currentBatchStartPosition = positionMarker.newPosition();
        channel.putLong( InputCache.END_OF_CACHE );
        channel.putInt( InputCache.NO_ENTITIES );
    }

    protected void clearState()
    {
        Arrays.fill( previousGroupIds, Group.GLOBAL.id() );
    }

    protected void writeEntity( ENTITY entity ) throws IOException
    {
        // properties
        if ( entity.hasFirstPropertyId() )
        {
            channel.putShort( HAS_FIRST_PROPERTY_ID ).putLong( entity.firstPropertyId() );
        }
        else
        {
            Object[] properties = entity.properties();
            channel.putShort( safeCastLongToShort( properties.length / 2 ) );
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

    protected void writeGroup( Group group, int slot ) throws IOException
    {
        if ( group.id() == previousGroupIds[slot] )
        {
            channel.put( SAME_GROUP );
        }
        else
        {
            channel.put( NEW_GROUP );
            channel.putInt( previousGroupIds[slot] = group.id() );
            writeToken( GROUP_TOKEN, group.name() );
        }
    }

    protected void writeValue( Object value ) throws IOException
    {
        ValueType type = ValueType.typeOf( value );
        channel.put( type.id() );
        type.write( value, channel );
    }

    protected void writeToken( byte type, Object key ) throws IOException
    {
        if ( key instanceof String )
        {
            Integer id = tokens[type].get( key );
            if ( id == null )
            {
                if ( nextKeyId[type] == maxKeyId[type] )
                {
                    throw new UnsupportedOperationException( "Too many tokens. Creation of more then " +
                                                        maxKeyId[type] + " tokens is not supported." );
                }
                tokens[type].put( (String) key, id = nextKeyId[type]++ );
                header.put( type );
                ValueType.stringType().write( key, header );
            }
            channel.putInt( id );
        }
        else if ( key instanceof Integer )
        {
            // Here we signal that we have a real token id, not to be confused by the local and contrived
            // token ids we generate in here. Following this -1 is the real token id.
            channel.putInt( (short) -1 );
            channel.putInt( (Integer) key );
        }
        else
        {
            throw new IllegalArgumentException( "Invalid key " + key + ", " + key.getClass() );
        }
    }

    @Override
    public void close() throws IOException
    {
        newBatch();

        header.put( END_OF_HEADER );
        // This is a special value denoting the end of the stream. This is done like this since
        // properties are the first thing read for every entity.
        channel.putShort( END_OF_ENTITIES );

        channel.close();
        header.close();
        storeChannel.close();
        headerChannel.close();
    }

    private void initMaxTokenKeyIds( RecordFormats recordFormats )
    {
        maxKeyId[PROPERTY_KEY_TOKEN] = getMaxAcceptableTokenId( recordFormats.propertyKeyToken().getMaxId() );
        maxKeyId[LABEL_TOKEN] = getMaxAcceptableTokenId( recordFormats.labelToken().getMaxId() );
        maxKeyId[RELATIONSHIP_TYPE_TOKEN] = getMaxAcceptableTokenId( recordFormats.relationshipTypeToken().getMaxId() );
        maxKeyId[GROUP_TOKEN] = getMaxAcceptableTokenId( recordFormats.relationshipGroup().getMaxId() );
    }

    private int getMaxAcceptableTokenId( long maxId )
    {
        return (int) Math.min( Integer.MAX_VALUE, maxId );
    }
}
