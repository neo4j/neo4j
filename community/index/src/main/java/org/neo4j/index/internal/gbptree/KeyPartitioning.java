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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class KeyPartitioning<KEY>
{
    private final Layout<KEY,?> layout;

    KeyPartitioning( Layout<KEY,?> layout )
    {
        this.layout = layout;
    }

    public List<Pair<KEY,KEY>> partition( Collection<KEY> keyCandidates, KEY fromInclusive, KEY toExclusive, int numberOfPartitions )
    {
        List<KEY> keys = keyCandidates.stream()
                .filter( key -> layout.compare( key, fromInclusive ) > 0 && layout.compare( key, toExclusive ) < 0 )
                .collect( Collectors.toList() );

        List<Pair<KEY,KEY>> partitions = new ArrayList<>();
        float stride = keys.size() < numberOfPartitions ? 1 : (1f + keys.size()) / numberOfPartitions;
        float pos = stride;
        KEY prev = fromInclusive;
        for ( int i = 0; i < numberOfPartitions - 1 && i < keys.size(); i++, pos += stride )
        {
            KEY split = keys.get( Math.round( pos ) - 1 );
            partitions.add( Pair.of( prev, split ) );
            prev = layout.newKey();
            layout.copyKey( split, prev );
        }
        partitions.add( Pair.of( prev, toExclusive ) );
        return partitions;
    }
}
