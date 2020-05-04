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
package org.neo4j.index.internal.gbptree;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;

class InternalTreeLogicDynamicSizeTest extends InternalTreeLogicTestBase<RawBytes,RawBytes>
{
    @Override
    protected ValueMerger<RawBytes,RawBytes> getAdder()
    {
        return ( existingKey, newKey, base, add ) ->
        {
            long baseSeed = layout.keySeed( base );
            long addSeed = layout.keySeed( add );
            RawBytes merged = layout.value( baseSeed + addSeed );
            base.copyFrom( merged );
            return ValueMerger.MergeResult.MERGED;
        };
    }

    @Override
    protected TreeNode<RawBytes,RawBytes> getTreeNode( int pageSize, Layout<RawBytes,RawBytes> layout, OffloadStore<RawBytes,RawBytes> offloadStore )
    {
        return new TreeNodeDynamicSize<>( pageSize, layout, offloadStore );
    }

    @Override
    protected TestLayout<RawBytes,RawBytes> getLayout()
    {
        return new SimpleByteArrayLayout();
    }

    @Test
    void shouldFailToInsertTooLargeKeys()
    {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[node.keyValueSizeCap() + 1];
        value.bytes = new byte[0];

        shouldFailToInsertTooLargeKeyAndValue( key, value );
    }

    @Test
    void shouldFailToInsertTooLargeKeyAndValueLargeKey()
    {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[node.keyValueSizeCap()];
        value.bytes = new byte[1];

        shouldFailToInsertTooLargeKeyAndValue( key, value );
    }

    @Test
    void shouldFailToInsertTooLargeKeyAndValueLargeValue()
    {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[1];
        value.bytes = new byte[node.keyValueSizeCap()];

        shouldFailToInsertTooLargeKeyAndValue( key, value );
    }

    private void shouldFailToInsertTooLargeKeyAndValue( RawBytes key, RawBytes value )
    {
        initialize();
        var e = assertThrows( IllegalArgumentException.class, () -> insert( key, value ) );
        assertThat( e.getMessage() ).contains( "Index key-value size it to large. Please see index documentation for limitations." );
    }

    @Test
    void storeOnlyMinimalKeyDividerInInternal() throws IOException
    {
        // given
        initialize();
        long key = 0;
        while ( numberOfRootSplits == 0 )
        {
            insert( key( key ), value( key ) );
            key++;
        }

        // when
        RawBytes rawBytes = keyAt( root.id(), 0, INTERNAL );

        // then
        assertEquals( Long.BYTES, rawBytes.bytes.length, "expected no tail on internal key but was " + rawBytes );
    }
}
