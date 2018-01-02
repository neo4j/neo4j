/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.WritableLogChannel;

import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToShort;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_ENTITIES;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_HEADER;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_FIRST_PROPERTY_ID;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.NEW_GROUP;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.SAME_GROUP;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.TOKEN;

/**
 * Abstract class for caching {@link InputEntity} or derivative to disk using a binary format.
 */
abstract class InputEntityCacher<ENTITY extends InputEntity> implements Receiver<ENTITY[],IOException>
{
    protected final WritableLogChannel channel;
    private final WritableLogChannel header;
    private final StoreChannel storeChannel;
    private final StoreChannel headerChannel;
    private final int[] previousGroupIds;

    private short nextKeyId;
    private final Map<String,Short> tokens = new HashMap<>();

    protected InputEntityCacher( StoreChannel channel, StoreChannel header, int bufferSize, int groupSlots )
            throws IOException
    {
        this.storeChannel = channel;
        this.headerChannel = header;
        this.previousGroupIds = new int[groupSlots];
        for ( int i = 0; i < groupSlots; i++ )
        {
            previousGroupIds[i] = Group.GLOBAL.id();
        }
        // We don't really care about versions, it's just that apart from that the WritableLogChannel
        // does precisely what we want and there's certainly value in not duplicating that functionality.
        this.channel = new PhysicalWritableLogChannel(
                new PhysicalLogVersionedStoreChannel( channel, 0, (byte)0 ), bufferSize );
        this.header = new PhysicalWritableLogChannel(
                new PhysicalLogVersionedStoreChannel( header, 0, (byte)0 ), (int) ByteUnit.kibiBytes( 8 ) );
    }

    @Override
    public void receive( ENTITY[] batch ) throws IOException
    {
        for ( ENTITY entity : batch )
        {
            writeEntity( entity );
        }
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
            channel.putShort( safeCastLongToShort( properties.length/2 ) );
            for ( int i = 0; i < properties.length; i++ )
            {
                String key = (String) properties[i++];
                Object value = properties[i];
                if ( value == null )
                {
                    continue;
                }
                writeToken( key );
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
            writeToken( group.name() );
        }
    }

    protected void writeValue( Object value ) throws IOException
    {
        ValueType type = ValueType.typeOf( value );
        channel.put( type.id() );
        type.write( value, channel );
    }

    protected void writeToken( String key ) throws IOException
    {
        Short id = tokens.get( key );
        if ( id == null )
        {
            tokens.put( key, id = nextKeyId++ );
            header.put( TOKEN );
            ValueType.stringType().write( key, header );
        }
        channel.putShort( id );
    }

    @Override
    public void close() throws IOException
    {
        header.put( END_OF_HEADER );
        // This is a special value denoting the end of the stream. This is done like this since
        // properties are the first thing read for every entity.
        channel.putShort( END_OF_ENTITIES );

        channel.close();
        header.close();
        storeChannel.close();
        headerChannel.close();
    }
}
