/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.pagecache;

import java.util.HashMap;
import java.util.Map;

class ProfileRefCounts
{
    private static class Counter
    {
        private int count;

        void increment()
        {
            count++;
        }

        int decrementAndGet()
        {
            return --count;
        }
    }

    private final Map<Profile,Counter> bag;

    ProfileRefCounts()
    {
        bag = new HashMap<>();
    }

    synchronized void incrementRefCounts( Profile[] profiles )
    {
        for ( Profile profile : profiles )
        {
            bag.computeIfAbsent( profile, p -> new Counter() ).increment();
        }
    }

    synchronized void decrementRefCounts( Profile[] profiles )
    {
        for ( Profile profile : profiles )
        {
            bag.computeIfPresent( profile, ( p, i ) -> i.decrementAndGet() == 0 ? null : i );
        }
    }

    synchronized boolean contains( Profile profile )
    {
        return bag.containsKey( profile );
    }
}
