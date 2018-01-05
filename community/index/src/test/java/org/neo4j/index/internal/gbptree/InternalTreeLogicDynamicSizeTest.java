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
package org.neo4j.index.internal.gbptree;

public class InternalTreeLogicDynamicSizeTest extends InternalTreeLogicTestBase<RawBytes,RawBytes>
{
    private SimpleByteArrayLayout layout = new SimpleByteArrayLayout();

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
        return layout;
    }
}
