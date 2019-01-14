/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
            appendedCounts.merge( txId, 1, ( a, b ) -> a + b );
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
