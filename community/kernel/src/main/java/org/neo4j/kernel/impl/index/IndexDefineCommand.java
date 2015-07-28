/*
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandRecordVisitor;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;

import static java.lang.String.format;

import static org.neo4j.collection.primitive.Primitive.intObjectMap;

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
        setIndexNameIdRange( new HashMap<String,Integer>() );
        setKeyIdRange( new HashMap<String,Integer>() );
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
            result.put( entry.getValue().intValue(), entry.getKey() );
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
        if ( nextIdInt > HIGHEST_POSSIBLE_ID ) // >= since the actual value -1 is reserved for all-ones
        {
            throw new IllegalStateException( format(
                    "Modifying more than %d indexes in a single transaction is not supported",
                    HIGHEST_POSSIBLE_ID + 1 ) );
        }
        id = nextIdInt;

        stringToId.put( string, id );
        idToString.put( id, string );
        return id;
    }


    @Override
    public int hashCode()
    {
        int result = nextIndexNameId != null ? nextIndexNameId.hashCode() : 0;
        result = 31 * result + (nextKeyId != null ? nextKeyId.hashCode() : 0);
        result = 31 * result + (getIndexNameIdRange() != null ? getIndexNameIdRange().hashCode() : 0);
        result = 31 * result + (getKeyIdRange() != null ? getKeyIdRange().hashCode() : 0);
        result = 31 * result + (idToIndexName != null ? idToIndexName.hashCode() : 0);
        result = 31 * result + (idToKey != null ? idToKey.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        IndexDefineCommand other = (IndexDefineCommand) obj;
        return getIndexNameIdRange().equals( other.getIndexNameIdRange() ) &&
                getKeyIdRange().equals( other.getKeyIdRange() );
    }

    @Override
    public void accept( CommandRecordVisitor visitor )
    {
        // no op
    }

    @Override
    public boolean handle( NeoCommandHandler visitor ) throws IOException
    {
        return visitor.visitIndexDefineCommand( this );
    }

    public Map<String,Integer> getIndexNameIdRange()
    {
        return indexNameIdRange;
    }

    public void setIndexNameIdRange( Map<String,Integer> indexNameIdRange )
    {
        this.indexNameIdRange = indexNameIdRange;
    }

    public Map<String,Integer> getKeyIdRange()
    {
        return keyIdRange;
    }

    public void setKeyIdRange( Map<String,Integer> keyIdRange )
    {
        this.keyIdRange = keyIdRange;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[names:" + indexNameIdRange + ", keys:" + keyIdRange + "]";
    }
}
