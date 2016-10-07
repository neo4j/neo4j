/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.bptree;

import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

/**
 * These are knobs that control and change basic behavior of a {@link BPTreeIndex}, something which
 * is very useful to do when trying different things out. Of course these knobs could still be here
 * even after some of them become set in stone.
 */
public class Knobs
{
    /**
     * TRUE: inserting entries into nodes rearranges physical location of existing entries in
     * the node to keep the sorted order.
     * FALSE: inserting entries into nodes appends to the end of the node and a small jump list
     * is updated with logical -> physical position information, which will be used when reading.
     *
     * Having this TRUE will make recovery much more feasible, at least require way less data in
     * WAL log records.
     */
    public static final boolean PHYSICALLY_SORTED_ENTRIES =
            FeatureToggles.flag( Knobs.class, "PHYSICALLY_SORTED_ENTRIES", false );

    /**
     * TRUE: when splitting a tree node two new nodes are created, leaving the old node intact
     * and simply re-links parent and sibling links to the two new nodes.
     * FALSE: when splitting a tree node only one new node will be created and the current node
     * will become the left node in the split.
     *
     * Having this TRUE will make recovery much more feasible, at least require way less data in
     * WAL log records.
     */
    public static final boolean SPLIT_KEEPS_SOURCE_INTACT =
            FeatureToggles.flag( Knobs.class, "SPLIT_KEEPS_SOURCE_INTACT", true );
}
