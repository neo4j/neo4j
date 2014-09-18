/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.fs.FileSystemAbstraction;

import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;

public class CountsStore
{
    private final ConcurrentMap<Key, AtomicLong> counts = new ConcurrentHashMap<>();

    public void close()
    {
        // TODO: assert that we don't have any memory state - because we should have seen rotation before this...
    }

    public static void createEmptyCountsStore( FileSystemAbstraction fs, File file, String version )
    {
        // TODO: store this to disk
    }

    public long countsForNode( int labelId )
    {
        return get( new NodeKey( labelId ) );
    }

    public void updateCountsForNode( int labelId, long delta )
    {
        update( new NodeKey( labelId ), delta );
    }

    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        if ( startLabelId != ANY_LABEL || endLabelId != ANY_LABEL )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
        return get( new RelationshipKey( startLabelId, typeId, endLabelId ) );
    }

    public void updateCountsForRelationship( int startLabelId, int typeId, int endLabelId, long delta )
    {
        update( new RelationshipKey( startLabelId, typeId, endLabelId ), delta );
    }

    private long get( Key key )
    {
        AtomicLong count = counts.get( key );
        return count == null ? 0 : count.get();
    }

    private void update( Key key, long delta )
    {
        AtomicLong count = counts.get( key );
        if ( count == null )
        {
            AtomicLong proposal = new AtomicLong();
            count = counts.putIfAbsent( key, proposal );
            if ( count == null )
            {
                count = proposal;
            }
        }
        count.getAndAdd( delta );
    }

    private static abstract class Key
    {
        @Override
        public abstract int hashCode();

        @Override
        public abstract boolean equals( Object obj );
    }

    private static class NodeKey extends Key
    {
        private final int labelId;

        public NodeKey( int labelId )
        {
            this.labelId = labelId;
        }

        @Override
        public boolean equals( Object o )
        {
            return this == o || (o instanceof NodeKey) && labelId == ((NodeKey) o).labelId;
        }

        @Override
        public int hashCode()
        {
            return labelId;
        }
    }

    private static class RelationshipKey extends Key
    {
        private final int startLabelId;
        private final int typeId;
        private final int endLabelId;

        public RelationshipKey( int startLabelId, int typeId, int endLabelId )
        {
            this.startLabelId = startLabelId;
            this.typeId = typeId;
            this.endLabelId = endLabelId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( (o instanceof RelationshipKey) )
            {
                RelationshipKey that = (RelationshipKey) o;
                return endLabelId == that.endLabelId && startLabelId == that.startLabelId && typeId == that.typeId;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            int result = startLabelId;
            result = 31 * result + typeId;
            result = 31 * result + endLabelId;
            return result;
        }
    }
}
