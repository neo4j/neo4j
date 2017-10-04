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
package org.neo4j.causalclustering.core.consensus;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;

public class Followers
{

    private Followers()
    {
    }

    // TODO: This method is inefficient.. we should not have to update this state by a complete
    // TODO: iteration each time. Instead it should be updated as a direct response to each
    // TODO: append response.
    public static <MEMBER> long quorumAppendIndex( Set<MEMBER> votingMembers, FollowerStates<MEMBER> states )
    {
        /*
         * Build up a map of tx id -> number of instances that have appended,
         * sorted by tx id.
         *
         * This allows us to then iterate backwards over the values in the map,
         * adding up a total count of how many have appended, until we reach a majority.
         * Once we do, the tx id at the current entry in the map will be the highest one
         * with a majority appended.
         */

        TreeMap</* txId */Long, /* numAppended */Integer> appendedCounts = new TreeMap<>();
        for ( MEMBER member : votingMembers )
        {
            long txId = states.get( member ).getMatchIndex();
            Integer currentCount = appendedCounts.get( txId );
            if ( currentCount == null )
            {
                appendedCounts.put( txId, 1 );
            }
            else
            {
                appendedCounts.put( txId, currentCount + 1 );
            }
        }

        // Iterate over it until we find a majority
        int total = 0;
        for ( Map.Entry<Long, Integer> entry : appendedCounts.descendingMap().entrySet() )
        {
            total += entry.getValue();
            if ( MajorityIncludingSelfQuorum.isQuorum( votingMembers.size(), total ) )
            {
                return entry.getKey();
            }
        }

        // No majority for any appended entry
        return -1;
    }
}
