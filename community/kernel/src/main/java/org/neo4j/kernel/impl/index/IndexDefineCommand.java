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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.impl.api.CommandVisitor;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.NeoCommandType;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.Primitive.intObjectMap;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.write2bLengthAndString;

/**
 * A command which have to be first in the transaction. It will map index names
 * and keys to ids so that all other commands in that transaction only refer
 * to ids instead of names. This reduced the number of bytes needed for commands
 * roughly 50% for transaction with more than a couple of commands in it,
 * depending on the size of the value.
 *
 * After this command has been created it will act as a factory for other
 * commands so that it can spit out correct index name and key ids.
 */
public class IndexDefineCommand extends Command
{
    static final int HIGHEST_POSSIBLE_ID = 0xFFFF - 1; // -1 since the actual value -1 is reserved for all-ones
    private final AtomicInteger nextIndexNameId = new AtomicInteger();
    private final AtomicInteger nextKeyId = new AtomicInteger();
    private Map<String,Integer> indexNameIdRange;
    private Map<String,Integer> keyIdRange;
    private PrimitiveIntObjectMap<String> idToIndexName;
    private PrimitiveIntObjectMap<String> idToKey;

    public IndexDefineCommand()
    {
        setIndexNameIdRange( new HashMap<>() );
        setKeyIdRange( new HashMap<>() );
        idToIndexName = intObjectMap( 16 );
        idToKey = intObjectMap( 16 );
    }

    public void init( Map<String,Integer> indexNames, Map<String,Integer> keys )
    {
        this.setIndexNameIdRange( indexNames );
        this.setKeyIdRange( keys );
        idToIndexName = reverse( indexNames );
        idToKey = reverse( keys );
    }

    private static PrimitiveIntObjectMap<String> reverse( Map<String,Integer> map )
    {
        PrimitiveIntObjectMap<String> result = Primitive.intObjectMap( map.size() );
        for ( Map.Entry<String,Integer> entry : map.entrySet() )
        {
            result.put( entry.getValue(), entry.getKey() );
        }
        return result;
    }

    private static String getFromMap( PrimitiveIntObjectMap<String> map, int id )
    {
        if ( id == -1 )
        {
            return null;
        }
        String result = map.get( id );
        if ( result == null )
        {
            throw new IllegalArgumentException( "" + id );
        }
        return result;
    }

    public String getIndexName( int id )
    {
        return getFromMap( idToIndexName, id );
    }

    public String getKey( int id )
    {
        return getFromMap( idToKey, id );
    }

    public int getOrAssignIndexNameId( String indexName )
    {
        return getOrAssignId( indexNameIdRange, idToIndexName, nextIndexNameId, indexName );
    }

    public int getOrAssignKeyId( String key )
    {
        return getOrAssignId( keyIdRange, idToKey, nextKeyId, key );
    }

    private int getOrAssignId( Map<String,Integer> stringToId, PrimitiveIntObjectMap<String> idToString,
            AtomicInteger nextId, String string )
    {
        if ( string == null )
        {
            return -1;
        }

        Integer id = stringToId.get( string );
        if ( id != null )
        {
            return id;
        }

        int nextIdInt = nextId.incrementAndGet();
        if ( nextIdInt > HIGHEST_POSSIBLE_ID || stringToId.size() >= HIGHEST_POSSIBLE_ID )
        {
            throw new IllegalStateException( format(
                    "Modifying more than %d indexes or keys in a single transaction is not supported",
                    HIGHEST_POSSIBLE_ID + 1 ) );
        }
        id = nextIdInt;

        stringToId.put( string, id );
        idToString.put( id, string );
        return id;
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
        IndexDefineCommand that = (IndexDefineCommand) o;
        return nextIndexNameId.get() == that.nextIndexNameId.get() &&
               nextKeyId.get() == that.nextKeyId.get() &&
               Objects.equals( indexNameIdRange, that.indexNameIdRange ) &&
               Objects.equals( keyIdRange, that.keyIdRange ) &&
               Objects.equals( idToIndexName, that.idToIndexName ) &&
               Objects.equals( idToKey, that.idToKey );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), nextIndexNameId.get(), nextKeyId.get(), indexNameIdRange, keyIdRange,
                idToIndexName, idToKey );
    }

    @Override
    public boolean handle( CommandVisitor visitor ) throws IOException
    {
        return visitor.visitIndexDefineCommand( this );
    }

    public Map<String,Integer> getIndexNameIdRange()
    {
        return indexNameIdRange;
    }

    private void setIndexNameIdRange( Map<String,Integer> indexNameIdRange )
    {
        this.indexNameIdRange = indexNameIdRange;
    }

    public Map<String,Integer> getKeyIdRange()
    {
        return keyIdRange;
    }

    private void setKeyIdRange( Map<String,Integer> keyIdRange )
    {
        this.keyIdRange = keyIdRange;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[names:" + indexNameIdRange + ", keys:" + keyIdRange + "]";
    }

    @Override
    public void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( NeoCommandType.INDEX_DEFINE_COMMAND );
        byte zero = 0;
        IndexCommand.writeIndexCommandHeader( channel, zero, zero, zero, zero, zero, zero, zero );
        writeMap( channel, getIndexNameIdRange() );
        writeMap( channel, getKeyIdRange() );
    }

    private void writeMap( WritableChannel channel, Map<String,Integer> map ) throws IOException
    {
        assert map.size() <= IndexDefineCommand.HIGHEST_POSSIBLE_ID :
            "Can not write map with size larger than 2 bytes. Actual size " + map.size();
        channel.putShort( (short) map.size() );
        for ( Map.Entry<String,Integer> entry : map.entrySet() )
        {
            write2bLengthAndString( channel, entry.getKey() );
            int id = entry.getValue();
            assert id <= IndexDefineCommand.HIGHEST_POSSIBLE_ID :
                "Can not write id larger than 2 bytes. Actual value " + id;
            channel.putShort( (short) id );
        }
    }
}
