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
package org.neo4j.index.internal.gbptree;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertThat;

public class InternalTreeLogicDynamicSizeTest extends InternalTreeLogicTestBase<RawBytes,RawBytes>
{
    @Override
    protected ValueMerger<RawBytes,RawBytes> getAdder()
    {
        return ( existingKey, newKey, base, add ) ->
        {
            long baseSeed = layout.keySeed( base );
            long addSeed = layout.keySeed( add );
            return layout.value( baseSeed + addSeed );
        };
    }

    @Override
    protected TreeNode<RawBytes,RawBytes> getTreeNode( int pageSize, Layout<RawBytes,RawBytes> layout )
    {
        return new TreeNodeDynamicSize<>( pageSize, layout );
    }

    @Override
    protected TestLayout<RawBytes,RawBytes> getLayout()
    {
        return new SimpleByteArrayLayout();
    }

    @Test
    public void shouldFailToInsertTooLargeKeys() throws IOException
    {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[node.keyValueSizeCap() + 1];
        value.bytes = new byte[0];

        shouldFailToInsertTooLargeKeyAndValue( key, value );
    }

    @Test
    public void shouldFailToInsertTooLargeKeyAndValueLargeKey() throws IOException
    {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[node.keyValueSizeCap()];
        value.bytes = new byte[1];

        shouldFailToInsertTooLargeKeyAndValue( key, value );
    }

    @Test
    public void shouldFailToInsertTooLargeKeyAndValueLargeValue() throws IOException
    {
        RawBytes key = layout.newKey();
        RawBytes value = layout.newValue();
        key.bytes = new byte[1];
        value.bytes = new byte[node.keyValueSizeCap()];

        shouldFailToInsertTooLargeKeyAndValue( key, value );
    }

    private void shouldFailToInsertTooLargeKeyAndValue( RawBytes key, RawBytes value ) throws IOException
    {
        initialize();
        try
        {
            insert( key, value );
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(), CoreMatchers.containsString( "Index key-value size it to large. Please see index documentation for limitations." ) );
        }
    }
}
