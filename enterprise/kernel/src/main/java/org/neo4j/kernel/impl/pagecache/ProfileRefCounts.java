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
