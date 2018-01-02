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
package org.neo4j.kernel.api.impl.index;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;

public class LuceneNodeLabelRangeTest
{
    @Test
    public void shouldTransposeNodeIdsAndLabelIds() throws Exception
    {
        // given
        long[] labelIds = new long[]{1, 3, 5};
        long[][] nodeIdsByLabelIndex = new long[][]{{0}, {2, 4}, {0, 2, 4}};

        // when
        LuceneNodeLabelRange range = LuceneNodeLabelRange.fromBitmapStructure( 0, labelIds, nodeIdsByLabelIndex );

        // then
        assertThat( asIterable( range.nodes() ), hasItems(0L, 2L, 4L));
        assertThat( asIterable( range.labels( 0 ) ), hasItems( 1L, 5L ) );
        assertThat( asIterable( range.labels( 2 ) ), hasItems( 3L, 5L ) );
        assertThat( asIterable( range.labels( 4 ) ), hasItems( 3L, 5L ) );
    }
}
