/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.util.Objects;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static java.lang.Math.toIntExact;

/**
 * An in-memory single-threaded counts holder useful for modifying and reading counts transaction state.
 */
public class CountsDelta implements CountsAccessor, CountsAccessor.Updater
{
    private static final long DEFAULT_COUNT = 0;
    protected final LongLongHashMap nodeCounts = new LongLongHashMap();
    protected final MutableMap<RelationshipKey,MutableLong> relationshipCounts = UnifiedMap.newMap();

    @Override
    public long nodeCount( int labelId, PageCursorTracer cursorTracer )
    {
        return nodeCounts.getIfAbsent( labelId, DEFAULT_COUNT );
    }

    @Override
    public void incrementNodeCount( long labelId, long delta )
    {
        if ( delta != 0 )
        {
            nodeCounts.updateValue( labelId, DEFAULT_COUNT, l -> l + delta );
        }
    }

    @Override
    public long relationshipCount( int startLabelId, int typeId, int endLabelId, PageCursorTracer cursorTracer )
    {
        RelationshipKey relationshipKey = new RelationshipKey( startLabelId, typeId, endLabelId );
        MutableLong counts = relationshipCounts.get( relationshipKey );
        return counts == null ? 0 : counts.longValue();
    }

    @Override
    public void incrementRelationshipCount( long startLabelId, int typeId, long endLabelId, long delta )
    {
        if ( delta != 0 )
        {
            RelationshipKey relationshipKey = new RelationshipKey( toIntExact( startLabelId ), typeId, toIntExact( endLabelId ) );
            relationshipCounts.getIfAbsentPutWithKey( relationshipKey, k -> new MutableLong( DEFAULT_COUNT ) ).add( delta );
        }
    }

    @Override
    public void close()
    {
        // this is close() of CountsAccessor.Updater - do nothing.
    }

    @Override
    public void accept( CountsVisitor visitor, PageCursorTracer cursorTracer )
    {
        nodeCounts.forEachKeyValue( ( id, count ) -> visitor.visitNodeCount( toIntExact( id ), count ) );
        relationshipCounts.forEachKeyValue( ( k, count ) -> visitor.visitRelationshipCount( k.startLabelId, k.typeId, k.endLabelId, count.longValue() ) );
    }

    public boolean hasChanges()
    {
        return !nodeCounts.isEmpty() || !relationshipCounts.isEmpty();
    }

    public static class RelationshipKey
    {
        public final int startLabelId;
        public final int typeId;
        public final int endLabelId;

        RelationshipKey( int startLabelId, int typeId, int endLabelId )
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
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            RelationshipKey that = (RelationshipKey) o;
            return startLabelId == that.startLabelId &&
                   typeId == that.typeId &&
                   endLabelId == that.endLabelId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( startLabelId, typeId, endLabelId );
        }
    }
}
