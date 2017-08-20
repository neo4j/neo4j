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
package org.neo4j.index.internal.gbptree;

/**
 * Responsible for instantiating correct {@link TreeNode} based on a format version.
 */
class TreeNodes
{
    static <KEY,VALUE> TreeNode<KEY,VALUE> instantiateTreeNode(
            int pageSize, Layout<KEY,VALUE> layout )
    {
        return instantiateTreeNode( GBPTree.FORMAT_VERSION, pageSize, layout );
    }

    static <KEY,VALUE> TreeNode<KEY,VALUE> instantiateTreeNode(
            int formatVersion, int pageSize, Layout<KEY,VALUE> layout )
    {
        switch ( formatVersion )
        {
        case 2:
            return new TreeNodeV2<>( pageSize, layout );
        case 3:
            return new TreeNodeV3<>( pageSize, layout );
        default:
            throw new IllegalArgumentException( "Tried to open a tree with unknown format version" );

        }
    }

    // TODO: method for migration too?
}
