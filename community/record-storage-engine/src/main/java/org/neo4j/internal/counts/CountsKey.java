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
package org.neo4j.internal.counts;

import java.util.Objects;

import org.neo4j.counts.CountsVisitor;
import org.neo4j.index.internal.gbptree.GBPTree;

import static java.lang.String.format;

/**
 * Key in a {@link GBPTree} owned by {@link GBPTreeCountsStore}.
 */
public class CountsKey
{
    static final int SIZE = Byte.BYTES +    // type
                            Long.BYTES +    // long for main data
                            Integer.BYTES;  // int for additional data

    /**
     * Key data layout for this type:
     * <pre>
     * first:  8B txId
     * second: 0
     * </pre>
     */
    private static final byte TYPE_STRAY_TX_ID = 0;

    /**
     * Key data layout for this type:
     * <pre>
     * first:  4B (lsb) labelId
     * second: 0
     * </pre>
     */
    private static final byte TYPE_NODE = 1;

    /**
     * Key data layout for this type:
     * <pre>
     * first:  4B (msb) startLabelId, 4B (lsb) relationshipTypeId
     * second: 4B endLabelId
     * </pre>
     */
    private static final byte TYPE_RELATIONSHIP = 2;

    // Commonly used keys
    static final CountsKey MIN_COUNT = new CountsKey( TYPE_NODE, Long.MIN_VALUE, Integer.MIN_VALUE );
    static final CountsKey MAX_COUNT = new CountsKey( TYPE_RELATIONSHIP, Long.MAX_VALUE, Integer.MAX_VALUE );
    static final CountsKey MIN_STRAY_TX_ID = strayTxId( Long.MIN_VALUE );
    static final CountsKey MAX_STRAY_TX_ID = strayTxId( Long.MAX_VALUE );

    /**
     * Type of key, as defined by "TYPE_" constants in this class.
     */
    byte type;

    /**
     * First 8B of the key data. Depending on {@link #type} these bytes mean different things.
     * Keeping the layout fixed and always 12B (these bytes plus 4B from {@link #second} simplified some aspects of reading, writing and working with the data.
     */
    long first;

    /**
     * Additional 4B of key data. Depending on {@link #type} these bytes mean different things or are unused.
     */
    int second;

    CountsKey()
    {
    }

    CountsKey( byte type, long keyFirst, int keySecond )
    {
        initialize( type, keyFirst, keySecond );
    }

    void initialize( byte type, long keyFirst, int keySecond )
    {
        this.type = type;
        this.first = keyFirst;
        this.second = keySecond;
    }

    /**
     * Public utility method for instantiating a {@link CountsKey} for a node label id.
     * @param labelId id of the label.
     * @return a {@link CountsKey for the node label id. The returned key can be put into {@link Map maps} and similar.
     */
    public static CountsKey nodeKey( long labelId )
    {
        return new CountsKey( TYPE_NODE, labelId, 0 );
    }

    /**
     * Public utility method for instantiating a {@link CountsKey} for a node start/end label and relationship type id.
     * @param startLabelId id of the label of start node.
     * @param typeId id of the relationship type.
     * @param endLabelId id of the label of end node.
     * @return a {@link CountsKey for the node start/end label and relationship type id. The returned key can be put into {@link Map maps} and similar.
     */
    public static CountsKey relationshipKey( long startLabelId, long typeId, long endLabelId )
    {
        return new CountsKey( TYPE_RELATIONSHIP, (startLabelId << Integer.SIZE) | (typeId & 0xFFFFFFFFL), (int) endLabelId );
    }

    static CountsKey strayTxId( long txId )
    {
        return new CountsKey( TYPE_STRAY_TX_ID, txId, 0 );
    }

    // Implements hashCode/equals so that these instances can be keys in a map
    @Override
    public int hashCode()
    {
        return Objects.hash( type, first, second );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( !(obj instanceof CountsKey) )
        {
            return false;
        }
        CountsKey other = (CountsKey) obj;
        return type == other.type && first == other.first && second == other.second;
    }

    void accept( CountsVisitor visitor, long count )
    {
        switch ( type )
        {
        case TYPE_NODE:
            visitor.visitNodeCount( (int) first, count );
            break;
        case TYPE_RELATIONSHIP:
            visitor.visitRelationshipCount( extractStartLabelId(), (int) first, second, count );
            break;
        default:
            throw new IllegalArgumentException( "Unknown key type " + type );
        }
    }

    private int extractStartLabelId()
    {
        return (int) (first >>> Integer.SIZE);
    }

    @Override
    public String toString()
    {
        switch ( type )
        {
        case TYPE_NODE:
            return format( "Node[label:%d]", first );
        case TYPE_RELATIONSHIP:
            return format( "Relationship[startLabel:%d, type:%d, endLabel:%d]", extractStartLabelId(), (int) first, second );
        case TYPE_STRAY_TX_ID:
            return format( "Stray tx id:%d", first );
        default:
            return format( "Unknown key type:%d, first:%d, second:%d", type, first, second );
        }
    }
}
