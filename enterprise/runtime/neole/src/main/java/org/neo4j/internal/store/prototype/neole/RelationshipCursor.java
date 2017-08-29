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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.internal.store.cursors.ReadCursor;

import static org.neo4j.internal.store.prototype.neole.PartialPropertyCursor.NO_PROPERTIES;
import static org.neo4j.internal.store.prototype.neole.ReadStore.combineReference;

abstract class RelationshipCursor extends ReadCursor implements RelationshipDataAccessor
{
    /**
     * <pre>
     *  0: in_use, high node bits (1 bytes)
     *  1: first_node             (4 bytes)
     *  5: second_node            (4 bytes)
     *  9: high chain bits        (2 bytes)
     * 11: rel_type               (2 bytes)
     * 13: first_prev_rel_id      (4 bytes)
     * 17: first_next_rel_id      (4 bytes)
     * 21: second_prev_rel_id     (4 bytes)
     * 25: second_next_rel_id     (4 bytes)
     * 29: next_prop_id           (4 bytes)
     * 33: first-in-chain-markers (1 bytes)
     * </pre>
     * <h2>high node bits</h2>
     * <pre>
     * [    ,   x] in use
     * [    ,xxx ] source node high bits
     * [xxxx,    ] property high bits
     * </pre>
     * <h2>high chain bits</h2>
     * <pre>
     * [ xxx,    ] [    ,    ] second node high order bits,     0x7000
     * [    ,xxx ] [    ,    ] first prev rel high order bits,  0x0E00
     * [    ,   x] [xx  ,    ] first next rel high order bits,  0x01C0
     * [    ,    ] [  xx,x   ] second prev rel high order bits, 0x0038
     * [    ,    ] [    , xxx] second next rel high order bits, 0x0007
     * </pre>
     */
    static final int RECORD_SIZE = 34;
    static final long NO_RELATIONSHIP = -1;
    protected final ReadStore store;

    RelationshipCursor( ReadStore store )
    {
        this.store = store;
    }

    @Override
    protected int dataBound()
    {
        return RECORD_SIZE;
    }

    boolean inUse()
    {
        return (unsignedByte( 0 ) & 0x01) != 0;
    }

    @Override
    public long relationshipReference()
    {
        return virtualAddress();
    }

    @Override
    public int label()
    {
        return unsignedShort( 11 );
    }

    @Override
    public long sourceNodeReference()
    {
        return combineReference( unsignedInt( 1 ), ((long) unsignedByte( 0 ) & 0x0EL) << 31 );
    }

    @Override
    public long targetNodeReference()
    {
        return combineReference( unsignedInt( 5 ), ((long) unsignedShort( 9 ) & 0x70L) << 20 );
    }

    @Override
    public long propertiesReference()
    {
        return combineReference( unsignedInt( 29 ), ((long) unsignedByte( 0 ) & 0xF0L) << 28 );
    }

    long sourcePrevRelationshipReference()
    {
        return combineReference( unsignedInt( 13 ), ((long) unsignedShort( 9 ) & 0x0E00L) << 23 );
    }

    long sourceNextRelationshipReference()
    {
        return combineReference( unsignedInt( 17 ), ((long) unsignedShort( 9 ) & 0x01C0L) << 26 );
    }

    long targetPrevRelationshipReference()
    {
        return combineReference( unsignedInt( 21 ), ((long) unsignedShort( 9 ) & 0x0038L) << 29 );
    }

    long targetNextRelationshipReference()
    {
        return combineReference( unsignedInt( 25 ), ((long) unsignedShort( 9 ) & 0x0007L) << 32 );
    }

    @Override
    public boolean hasProperties()
    {
        return propertiesReference() != NO_PROPERTIES;
    }

    @Override
    public void source( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        store.singleNode( sourceNodeReference(), cursor );
    }

    @Override
    public void target( NodeCursor cursor )
    {
        store.singleNode( targetNodeReference(), cursor );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        store.relationshipProperties( propertiesReference(), cursor );
    }
}
