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
package org.neo4j.internal.store.prototype.neole;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.store.cursors.ReadCursor;

import static org.neo4j.internal.store.prototype.neole.PartialPropertyCursor.NO_PROPERTIES;
import static org.neo4j.internal.store.prototype.neole.ReadStore.combineReference;
import static org.neo4j.internal.store.prototype.neole.RelationshipCursor.NO_RELATIONSHIP;
import static org.neo4j.internal.store.prototype.neole.RelationshipGroupCursor.encodeDirectRelationshipReference;

class NodeCursor extends ReadCursor implements org.neo4j.internal.kernel.api.NodeCursor
{
    /**
     * <pre>
     *  0: in use        (1 byte)
     *  1: relationships (4 bytes)
     *  5: properties    (4 bytes)
     *  9: labels        (5 bytes)
     * 14: extra         (1 byte)
     * </pre>
     * <h2>in use</h2>
     * <pre>
     * [    ,   x] in use
     * [    ,xxx ] high(relationships)
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
    protected final ReadStore store;
    private long maxReference;

    NodeCursor( ReadStore store )
    {
        this.store = store;
    }

    void init( StoreFile nodes, long reference, long maxReference )
    {
        nodes.initializeCursor( reference, this );
        initializeScanCursor();
        this.maxReference = maxReference;
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
        return false;
    }

    @Override
    public long nodeReference()
    {
        return virtualAddress();
    }

    // DATA ACCESSOR METHODS

    private boolean inUse()
    {
        return (unsignedByte( 0 ) & 0x01) != 0;
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
    public long relationshipGroupReference()
    {
        long relationships = combineReference( unsignedInt( 1 ), (unsignedByte( 0 ) & 0x0EL) << 31 );
        if ( (readByte( 14 ) & 0x01) != 0 )
        {
            return relationships;
        }
        else if ( relationships == NO_RELATIONSHIP )
        {
            return NO_RELATIONSHIP;
        }
        else
        {
            return encodeDirectRelationshipReference( relationships );
        }
    }

    @Override
    public long allRelationshipsReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long propertiesReference()
    {
        return combineReference( unsignedInt( 5 ), (unsignedByte( 0 ) & 0xF0L) << 28 );
    }

    @Override
    public boolean isDense()
    {
        return true;
    }

    // ===================

    @Override
    protected int dataBound()
    {
        return RECORD_SIZE;
    }

    @Override
    public boolean hasProperties()
    {
        return propertiesReference() != NO_PROPERTIES;
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        store.relationshipGroups( nodeReference(), relationshipGroupReference(), cursor );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor relationships )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        store.nodeProperties( propertiesReference(), cursor );
    }
}
