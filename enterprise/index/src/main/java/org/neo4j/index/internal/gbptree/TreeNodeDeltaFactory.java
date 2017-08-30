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
package org.neo4j.index.internal.gbptree;

public class TreeNodeDeltaFactory extends TreeNodeFactory
{
    private static final int PRIORITY = TreeNodeSimpleFactory.PRIORITY + 1;

    public TreeNodeDeltaFactory()
    {
        super( TreeNodeDeltaFactory.class.getName(), TreeNodeDelta.FORMAT_IDENTIFIER, TreeNodeDelta.FORMAT_VERSION, PRIORITY );
    }

    @Override
    <KEY, VALUE> TreeNode<KEY,VALUE> instantiate( int pageSize, Layout<KEY,VALUE> layout )
    {
        return new TreeNodeDelta<>( pageSize, layout );
    }
}
