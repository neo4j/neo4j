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

import org.apache.commons.lang3.mutable.MutableLong;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;

public class TreeNodeFixedSizeTest extends TreeNodeTestBase<MutableLong,MutableLong>
{
    private final SimpleLongLayout layout = longLayout().build();

    @Override
    protected TestLayout<MutableLong,MutableLong> getLayout()
    {
        return layout;
    }

    @Override
    protected TreeNode<MutableLong,MutableLong> getNode( int pageSize, Layout<MutableLong,MutableLong> layout )
    {
        return new TreeNodeFixedSize<>( pageSize, layout );
    }

    @Override
    void assertAdditionalHeader( PageCursor cursor, TreeNode<MutableLong,MutableLong> node, int pageSize )
    {   // no addition header
    }
}
