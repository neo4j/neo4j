/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.storageengine.api.TokenIndexEntryUpdate;

class PhysicalToLogicalTokenChanges2
{
    private PhysicalToLogicalTokenChanges2()
    {
    }

    /**
     * Converts physical before/after state to logical remove/add state. This conversion reuses the existing
     * long[] arrays in {@link TokenIndexEntryUpdate}, 'before' is used for removals and 'values' is used for adds,
     * by shuffling numbers around and possible terminates them with -1 because the logical change set will be
     * equally big or smaller than the physical change set.
     *
     * @param update {@link TokenIndexEntryUpdate} containing physical before/after state.
     */
    static void convertToAdditionsAndRemovals( TokenIndexEntryUpdate<?> update )
    {
        int beforeLength = update.beforeValues().length;
        int afterLength = update.values().length;

        int bc = 0;
        int ac = 0;
        long[] before = update.beforeValues();
        long[] after = update.values();
        for ( int bi = 0, ai = 0; bi < beforeLength || ai < afterLength; )
        {
            long beforeId = bi < beforeLength ? before[bi] : -1;
            long afterId = ai < afterLength ? after[ai] : -1;
            if ( beforeId == afterId )
            {   // no change
                bi++;
                ai++;
                continue;
            }

            if ( smaller( beforeId, afterId ) )
            {
                while ( smaller( beforeId, afterId ) && bi < beforeLength )
                {
                    // looks like there's an id in before which isn't in after ==> REMOVE
                    update.beforeValues()[bc++] = beforeId;
                    bi++;
                    beforeId = bi < beforeLength ? before[bi] : -1;
                }
            }
            else if ( smaller( afterId, beforeId ) )
            {
                while ( smaller( afterId, beforeId ) && ai < afterLength )
                {
                    // looks like there's an id in after which isn't in before ==> ADD
                    update.values()[ac++] = afterId;
                    ai++;
                    afterId = ai < afterLength ? after[ai] : -1;
                }
            }
        }

        terminateWithMinusOneIfNeeded( update.beforeValues(), bc );
        terminateWithMinusOneIfNeeded( update.values(), ac );
    }

    private static boolean smaller( long id, long otherId )
    {
        return id != -1 && (otherId == -1 || id < otherId);
    }

    private static void terminateWithMinusOneIfNeeded( long[] tokenIds, int actualLength )
    {
        if ( actualLength < tokenIds.length )
        {
            tokenIds[actualLength] = -1;
        }
    }
}
