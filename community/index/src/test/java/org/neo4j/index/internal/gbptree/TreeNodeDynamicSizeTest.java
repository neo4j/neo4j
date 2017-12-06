package org.neo4j.index.internal.gbptree;

import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;

/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
public class TreeNodeDynamicSizeTest extends TreeNodeTestBase<RawBytes,RawBytes>
{
    private SimpleByteArrayLayout layout = new SimpleByteArrayLayout();

    @Override
    protected Layout<RawBytes,RawBytes> getLayout()
    {
        return layout;
    }

    @Override
    protected TreeNode<RawBytes,RawBytes> getNode( int pageSize, Layout<RawBytes,RawBytes> layout )
    {
        return new TreeNodeDynamicSize<>( pageSize, layout );
    }

    @Override
    protected RawBytes key( long sortOrder )
    {
        RawBytes key = layout.newKey();
        key.bytes = ByteBuffer.allocate( 16 ).putLong( sortOrder ).putLong( sortOrder ).array();
        return key;
    }

    @Override
    protected RawBytes value( long someValue )
    {
        RawBytes value = layout.newValue();
        value.bytes = ByteBuffer.allocate( 17 ).putLong( someValue ).putLong( someValue ).array();
        return value;
    }

    @Override
    boolean valuesEqual( RawBytes firstValue, RawBytes secondValue )
    {
        return layout.compare( firstValue, secondValue ) == 0;
    }

    @Override
    void assertAdditionalHeader( PageCursor cursor, TreeNode<RawBytes,RawBytes> node, int pageSize )
    {
        // When
        int currentAllocSpace = ((TreeNodeDynamicSize) node).getAllocSpace( cursor );

        // Then
        assertEquals("allocSpace point to end of page", pageSize, currentAllocSpace );
    }
}
