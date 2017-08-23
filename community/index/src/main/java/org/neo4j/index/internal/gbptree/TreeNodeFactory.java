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

import org.neo4j.helpers.Service;

/**
 * Service loading bridge for loading a {@link TreeNode}.
 */
public abstract class TreeNodeFactory extends Service implements Comparable<TreeNodeFactory>
{
    private final int priority;
    private final byte formatIdentifier;
    private final byte formatVersion;

    TreeNodeFactory( String name, byte formatIdentifier, byte formatVersion, int priority )
    {
        super( name );
        this.formatIdentifier = formatIdentifier;
        this.formatVersion = formatVersion;
        this.priority = priority;
    }

    @SuppressWarnings( "unchecked" )
    abstract <KEY,VALUE> TreeNode<KEY,VALUE> instantiate( int pageSize, Layout<KEY,VALUE> layout );

    byte formatIdentifier()
    {
        return formatIdentifier;
    }

    byte formatVersion()
    {
        return formatVersion;
    }

    @Override
    public int compareTo( TreeNodeFactory o )
    {
        // Sorting a list with factories will have the highest prioritized as element 0
        return Integer.compare( o.priority, priority );
    }
}
