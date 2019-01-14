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
package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.impl.store.kvstore.WritableBuffer;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;

class KeyFormat implements CountsVisitor
{
    private static final byte NODE_COUNT = 1;
    private static final byte RELATIONSHIP_COUNT = 2;
    private static final byte INDEX = 127;
    private static final byte INDEX_STATS = 1;
    private static final byte INDEX_SAMPLE = 2;
    private final WritableBuffer buffer;

    KeyFormat( WritableBuffer key )
    {
        assert key.size() >= 16;
        this.buffer = key;
    }

    /**
     * Key format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [t,0,0,0,0,0,0,0 ; 0,0,0,0,l,l,l,l]
     *  t - entry type - "{@link #NODE_COUNT}"
     *  l - label id
     * </pre>
     * For value format, see {@link org.neo4j.kernel.impl.store.counts.CountsUpdater#incrementNodeCount(int, long)}.
     */
    @Override
    public void visitNodeCount( int labelId, long count )
    {
        buffer.putByte( 0, NODE_COUNT )
              .putInt( 12, labelId );
    }

    /**
     * Key format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [t,0,0,0,s,s,s,s ; r,r,r,r,e,e,e,e]
     *  t - entry type - "{@link #RELATIONSHIP_COUNT}"
     *  s - start label id
     *  r - relationship type id
     *  e - end label id
     * </pre>
     * For value format, see {@link org.neo4j.kernel.impl.store.counts.CountsUpdater#incrementRelationshipCount(int, int, int, long)}
     */
    @Override
    public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
    {
        buffer.putByte( 0, RELATIONSHIP_COUNT )
              .putInt( 4, startLabelId )
              .putInt( 8, typeId )
              .putInt( 12, endLabelId );
    }

    /**
     * Key format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [t,0,0,0,i,i,i,i ; 0,0,0,0,0,0,0,k]
     *  t - index entry marker - "{@link #INDEX}"
     *  k - entry (sub)type - "{@link #INDEX_STATS}"
     *  i - index id
     * </pre>
     * For value format, see {@link org.neo4j.kernel.impl.store.counts.CountsUpdater#replaceIndexUpdateAndSize(long, long, long)}.
     */
    @Override
    public void visitIndexStatistics( long indexId, long updates, long size )
    {
        indexKey( INDEX_STATS, indexId );
    }

    /**
     * Key format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [t,0,0,0,i,i,i,i ; 0,0,0,0,0,0,0,k]
     *  t - index entry marker - "{@link #INDEX}"
     *  k - entry (sub)type - "{@link #INDEX_STATS}"
     *  i - index id
     * </pre>
     * For value format, see {@link org.neo4j.kernel.impl.store.counts.CountsUpdater#replaceIndexSample(long , long, long)}.
     */
    @Override
    public void visitIndexSample( long indexId, long unique, long size )
    {
        indexKey( INDEX_SAMPLE, indexId );
    }

    private void indexKey( byte indexKey, long indexId )
    {
        buffer.putByte( 0, INDEX )
              .putInt( 4, (int) indexId )
              .putByte( 15, indexKey );
    }

    public static CountsKey readKey( ReadableBuffer key ) throws UnknownKey
    {
        switch ( key.getByte( 0 ) )
        {
        case KeyFormat.NODE_COUNT:
            return CountsKeyFactory.nodeKey( key.getInt( 12 ) );
        case KeyFormat.RELATIONSHIP_COUNT:
            return CountsKeyFactory.relationshipKey( key.getInt( 4 ), key.getInt( 8 ), key.getInt( 12 ) );
        case KeyFormat.INDEX:
            byte indexKeyByte = key.getByte( 15 );
            long indexId = key.getInt( 4 );
            switch ( indexKeyByte )
            {
            case KeyFormat.INDEX_STATS:
                return indexStatisticsKey( indexId );
            case KeyFormat.INDEX_SAMPLE:
                return CountsKeyFactory.indexSampleKey( indexId );
            default:
                throw new IllegalStateException( "Unknown index key: " + indexKeyByte );
            }
        default:
            throw new UnknownKey( "Unknown key type: " + key );
        }
    }
}
