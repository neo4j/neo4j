/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.store.prototype.neole;

import org.neo4j.impl.kernel.api.LabelSet;
import org.neo4j.impl.kernel.api.result.ValueWriter;

import static org.neo4j.impl.store.prototype.EdgeCursor.NO_EDGE;
import static org.neo4j.impl.store.prototype.neole.ReadStore.combineReference;

class NodeCursor extends org.neo4j.impl.store.prototype.NodeCursor<ReadStore>
{
    /**
     * <pre>
     *  0: in use     (1 byte)
     *  1: edges      (4 bytes)
     *  5: properties (4 bytes)
     *  9: labels     (5 bytes)
     * 14: extra      (1 byte)
     * </pre>
     * <h2>in use</h2>
     * <pre>
     * [    ,   x] in use
     * [    ,xxx ] high(edges)
     * [xxxx,    ] high(properties)
     * </pre>
     * <h2>labels</h2>
     * byte order: <code>[3][2][1][0][4]</code> (4 is msb, 0 is lsb)
     * <pre>
     * [    ,    ] [    ,    ] [    ,    ] [    ,    ] [1   ,    ] reference to labels store (0x80_0000_0000)
     * [    ,    ] [    ,    ] [    ,    ] [    ,    ] [0xxx,    ] number of inlined labels  (0x70_0000_0000)
     * </pre>
     * <h2>extra</h2>
     * <pre>
     * [    ,   x] dense
     * </pre>
     */
    static final int RECORD_SIZE = 15;
    long maxReference;

    NodeCursor( ReadStore store )
    {
        super( store );
    }

    void init( StoreFile nodes, long reference, long maxReference )
    {
        ReadStore.setup( nodes, this, reference - 1 );
        this.maxReference = maxReference;
    }

    private boolean inUse()
    {
        return (unsignedByte( 0 ) & 0x01) != 0;
    }

    @Override
    public long nodeReference()
    {
        return virtualAddress();
    }

    @Override
    public LabelSet labels()
    {
        long field = unsignedInt( 9 ) | (((long) unsignedByte( 13 )) << 32);
        if ( (field & 0x80_0000_0000L) != 0 ) // reference to labels store
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
        else // inlined labels
        {
            int numberOfLabels = (int) ((field & 0x70_0000_0000L) >>> 36); // 0 - 7
            if ( numberOfLabels == 0 )
            {
                return LabelSet.NONE;
            }
            int bitsPerLabel = 36 / numberOfLabels; // 5 - 36
            int[] labels = new int[numberOfLabels];
            long mask = (1L << bitsPerLabel) - 1;
            for ( int i = 0; i < numberOfLabels; i++ )
            {
                labels[i] = (int) (field & mask);
                field >>= bitsPerLabel;
            }
            return new Labels( labels );
        }
    }

    @Override
    public void writeIdTo( ValueWriter target )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long edgeGroupReference()
    {
        long edges = combineReference( unsignedInt( 1 ), (unsignedByte( 0 ) & 0x0EL) << 31 );
        if ( (readByte( 14 ) & 0x01) != 0 )
        {
            return edges;
        }
        else if ( edges == NO_EDGE )
        {
            return NO_EDGE;
        }
        else
        {
            return EdgeGroupCursor.encodeDirectEdgeReference( edges );
        }
    }

    @Override
    public long propertiesReference()
    {
        return combineReference( unsignedInt( 5 ), (unsignedByte( 0 ) & 0xF0L) << 28 );
    }

    @Override
    public boolean next()
    {
        while ( scanNextByVirtualAddress( maxReference ) )
        {
            if ( inUse() )
            {
                return true;
            }
        }
        // TODO: fetch next block
        return false;
    }

    @Override
    protected int dataBound()
    {
        return RECORD_SIZE;
    }
}
