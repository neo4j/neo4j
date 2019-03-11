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

import java.util.function.Supplier;

import org.neo4j.util.FeatureToggles;

/**
 * This class counts the number of round trips we take from the same node back to
 * root in a row and throw {@link TreeInconsistencyException} if it becomes too many.
 * <p>
 * Starting over from root happens when a seek cursor ended up on an unexpected node, such as
 * <ul>
 *  <li>Node is internal when we actually expect it to be a leaf.</li>
 *  <li>Generation of node is higher than the generation of the pointer that we followed to get here.</li>
 *  <li>Generation of the node differs from the generation we expected it to have based on previous lookup.</li>
 *  <li>Node is not a tree node.</li>
 * </ul>
 * All of those cases are either a result of a concurrent update changing the
 * structure of the tree, e.g. if the page we wanted to visit was reused for
 * something else before we got there, or the tree is in an inconsistent
 * state.
 * If the tree is inconsistent it is likely that we end up here again after
 * retrying from root and thus we will loop here forever. To avoid this we
 * keep track of the last page that we jumped to root from and if we loop
 * from the same place enough times we will throw an exception.
 */
public class TripCountingRootCatchup implements RootCatchup
{
    static final String MAX_TRIP_COUNT_NAME = "max_trip_count";
    static final int MAX_TRIP_COUNT_DEFAULT = 10;
    private final Supplier<Root> rootSupplier;
    private final int maxTripCount;
    private long lastFromId = TreeNode.NO_NODE_FLAG;
    private int tripCount;

    TripCountingRootCatchup( Supplier<Root> rootSupplier )
    {
        this.rootSupplier = rootSupplier;
        this.maxTripCount = FeatureToggles.getInteger( TripCountingRootCatchup.class, MAX_TRIP_COUNT_NAME, MAX_TRIP_COUNT_DEFAULT );
    }

    @Override
    public Root catchupFrom( long fromId )
    {
        updateTripCount( fromId );
        assertTripCount();
        return rootSupplier.get();
    }

    private void updateTripCount( long fromId )
    {
        if ( fromId == lastFromId )
        {
            tripCount++;
        }
        else
        {
            lastFromId = fromId;
            tripCount = 1;
        }
    }

    private void assertTripCount()
    {
        if ( tripCount >= maxTripCount )
        {
            throw new TreeInconsistencyException(
                    "Index traversal aborted due to being stuck in infinite loop. This is most likely caused by an inconsistency in the index. " +
                            "Loop occurred when restarting search from root from page %d.", lastFromId );
        }
    }
}
