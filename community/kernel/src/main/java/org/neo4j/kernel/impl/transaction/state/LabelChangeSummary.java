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
package org.neo4j.kernel.impl.transaction.state;

import static java.util.Arrays.binarySearch;
import static java.util.Arrays.copyOf;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class LabelChangeSummary
{
    private final long[] addedLabels;
    private final long[] removedLabels;
    private final long[] unchangedLabels;

    public LabelChangeSummary( long[] labelsBefore, long[] labelsAfter )
    {
        // Ids are sorted in the store
        long[] addedLabels = new long[labelsAfter.length];
        long[] removedLabels = new long[labelsBefore.length];
        long[] unchangedLabels = new long[Math.min( addedLabels.length, removedLabels.length )];
        int addedLabelsCursor = 0, removedLabelsCursor = 0, unchangedLabelsCursor = 0;
        for ( long labelAfter : labelsAfter )
        {
            if ( binarySearch( labelsBefore, labelAfter ) < 0 )
            {
                addedLabels[addedLabelsCursor++] = labelAfter;
            }
            else
            {
                unchangedLabels[unchangedLabelsCursor++] = labelAfter;
            }
        }
        for ( long labelBefore : labelsBefore )
        {
            if ( binarySearch( labelsAfter, labelBefore ) < 0 )
            {
                removedLabels[removedLabelsCursor++] = labelBefore;
            }
        }

        // For each property on the node, produce one update for added labels and one for removed labels.
        this.addedLabels = shrink( addedLabels, addedLabelsCursor );
        this.removedLabels = shrink( removedLabels, removedLabelsCursor );
        this.unchangedLabels = shrink( unchangedLabels, unchangedLabelsCursor );
    }

    private long[] shrink( long[] array, int toLength )
    {
        if ( toLength == 0 )
        {
            return EMPTY_LONG_ARRAY;
        }
        return array.length == toLength ? array : copyOf( array, toLength );
    }

    public boolean hasAddedLabels()
    {
        return addedLabels.length > 0;
    }

    public boolean hasRemovedLabels()
    {
        return removedLabels.length > 0;
    }

    public boolean hasUnchangedLabels()
    {
        return unchangedLabels.length > 0;
    }

    public long[] getAddedLabels()
    {
        return addedLabels;
    }

    public long[] getRemovedLabels()
    {
        return removedLabels;
    }

    public long[] getUnchangedLabels()
    {
        return unchangedLabels;
    }
}
