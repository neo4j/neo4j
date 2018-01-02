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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_ENTITIES;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_FIRST_PROPERTY_ID;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.NEW_GROUP;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.SAME_GROUP;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.TOKEN;

/**
 * Abstract class for reading cached entities previously stored using {@link InputEntityCacher} or derivative.
 */
abstract class InputEntityReader<ENTITY extends InputEntity> extends PrefetchingIterator<ENTITY>
        implements InputIterator<ENTITY>
{
    protected final ReadableLogChannel channel;
    private final LogPositionMarker positionMarker = new LogPositionMarker();
    private int lineNumber;
    private final Group[] previousGroups;
    private final PrimitiveIntObjectMap<String> tokens = Primitive.intObjectMap();

    InputEntityReader( StoreChannel channel, StoreChannel header, int bufferSize, int groupSlots ) throws IOException
    {
        this.previousGroups = new Group[groupSlots];
        for ( int i = 0; i < groupSlots; i++ )
        {
            previousGroups[i] = Group.GLOBAL;
        }
        this.channel = reader( channel, bufferSize );
        readHeader( header );
    }

    private ReadAheadLogChannel reader( StoreChannel channel, int bufferSize ) throws IOException
    {
        return new ReadAheadLogChannel(
                new PhysicalLogVersionedStoreChannel( channel, 0, (byte) 0 ), NO_MORE_CHANNELS, bufferSize );
    }

    private void readHeader( StoreChannel header ) throws IOException
    {
        try ( ReadableLogChannel reader = reader( header, (int) ByteUnit.kibiBytes( 8 ) ) )
        {
            for ( short id = 0; reader.get() == TOKEN; id++ )
            {
                tokens.put( id, (String) ValueType.stringType().read( reader ) );
            }
        }
    }

    @Override
    protected final ENTITY fetchNextOrNull()
    {
        try
        {
            lineNumber++;
            Object properties = readProperties();
            if ( properties == null )
            {
                return null;
            }

            return readNextOrNull( properties );
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't read cached node data", e );
        }
    }

    protected abstract ENTITY readNextOrNull( Object properties ) throws IOException;

    private Object readProperties() throws IOException
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
            Object[] properties = new Object[count*2];
            for ( int i = 0; i < properties.length; i++ )
            {
                properties[i++] = readToken();
                properties[i] = readValue();
            }
            return properties;
        }
    }

    protected String readToken() throws IOException
    {
        short id = channel.getShort();
        String name = tokens.get( id );
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
        case NEW_GROUP: return previousGroups[slot] = new Group.Adapter( channel.getInt(), readToken() );
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
            return channel.getCurrentPosition( positionMarker ).getByteOffset();
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
            channel.close();
        }
        catch ( IOException e )
        {
            throw new InputException( "Couldn't close channel for cached input data", e );
        }
    }
}
