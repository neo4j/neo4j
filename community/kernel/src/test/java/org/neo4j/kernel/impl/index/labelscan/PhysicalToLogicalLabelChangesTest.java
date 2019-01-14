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
package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;

import static org.junit.Assert.assertArrayEquals;

public class PhysicalToLogicalLabelChangesTest
{
    @Test
    public void shouldSeeSimpleAddition()
    {
        convertAndAssert(
                // before/after
                ids(), ids( 2 ),
                // removed/added
                ids(), ids( 2 ) );
    }

    @Test
    public void shouldSeeSimpleRemoval()
    {
        convertAndAssert(
                // before/after
                ids( 2 ), ids(),
                // removed/added
                ids( 2 ), ids() );
    }

    @Test
    public void shouldSeeSomeAdded()
    {
        convertAndAssert(
                // before/after
                ids( 1, 3, 5 ), ids( 1, 2, 3, 4, 5, 6 ),
                // removed/added
                ids(), ids( 2, 4, 6 ) );
    }

    @Test
    public void shouldSeeSomeRemoved()
    {
        convertAndAssert(
                // before/after
                ids( 1, 2, 3, 4, 5, 6 ), ids( 1, 3, 5 ),
                // removed/added
                ids( 2, 4, 6 ), ids() );
    }

    @Test
    public void shouldSeeSomeAddedAndSomeRemoved()
    {
        convertAndAssert(
                // before/after
                ids( 1, 3, 4, 6 ), ids( 0, 2, 3, 5, 6 ),
                // removed/added
                ids( 1, 4 ), ids( 0, 2, 5 ) );
    }

    private void convertAndAssert( long[] before, long[] after, long[] expectedRemoved, long[] expectedAdded )
    {
        NodeLabelUpdate update = NodeLabelUpdate.labelChanges( 0, before, after );
        PhysicalToLogicalLabelChanges.convertToAdditionsAndRemovals( update );
        assertArrayEquals( terminate( update.getLabelsBefore() ), expectedRemoved );
        assertArrayEquals( terminate( update.getLabelsAfter() ), expectedAdded );
    }

    private long[] terminate( long[] labels )
    {
        int length = actualLength( labels );
        return length == labels.length ? labels : Arrays.copyOf( labels, length );
    }

    private int actualLength( long[] labels )
    {
        for ( int i = 0; i < labels.length; i++ )
        {
            if ( labels[i] == -1 )
            {
                return i;
            }
        }
        return labels.length;
    }

    private long[] ids( long... ids )
    {
        return ids;
    }
}
