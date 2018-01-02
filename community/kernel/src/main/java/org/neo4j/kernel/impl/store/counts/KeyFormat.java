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
    private static final byte NODE_COUNT = 1, RELATIONSHIP_COUNT = 2, INDEX = 127, INDEX_STATS = 1, INDEX_SAMPLE = 2;
    private final WritableBuffer buffer;

    public KeyFormat( WritableBuffer key )
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
     * [t,0,0,0,l,l,l,l ; p,p,p,p,0,0,0,k]
     *  t - index entry marker - "{@link #INDEX}"
     *  k - entry (sub)type - "{@link #INDEX_STATS}"
     *  l - label id
     *  p - property key id
     * </pre>
     * For value format, see {@link org.neo4j.kernel.impl.store.counts.CountsUpdater#replaceIndexUpdateAndSize(int, int, long, long)}.
     */
    @Override
    public void visitIndexStatistics( int labelId, int propertyKeyId, long updates, long size )
    {
        indexKey( INDEX_STATS, labelId, propertyKeyId );
    }

    /**
     * Key format:
     * <pre>
     *  0 1 2 3 4 5 6 7   8 9 A B C D E F
     * [t,0,0,0,l,l,l,l ; p,p,p,p,0,0,0,k]
     *  t - index entry marker - "{@link #INDEX}"
     *  k - entry (sub)type - "{@link #INDEX_SAMPLE}"
     *  l - label id
     *  p - property key id
     * </pre>
     * For value format, see {@link org.neo4j.kernel.impl.store.counts.CountsUpdater#replaceIndexSample(int, int, long, long)}.
     */
    @Override
    public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
    {
        indexKey( INDEX_SAMPLE, labelId, propertyKeyId );
    }

    private void indexKey( byte indexKey, int labelId, int propertyKeyId )
    {
        buffer.putByte( 0, INDEX )
              .putInt( 4, labelId )
              .putInt( 8, propertyKeyId )
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
            switch ( key.getByte( 15 ) )
            {
            case KeyFormat.INDEX_STATS:
                return indexStatisticsKey( key.getInt( 4 ), key.getInt( 8 ) );
            case KeyFormat.INDEX_SAMPLE:
                return CountsKeyFactory.indexSampleKey( key.getInt( 4 ), key.getInt( 8 ) );
            }
        default:
            throw new UnknownKey( "Unknown key type: " + key );
        }
    }
}
