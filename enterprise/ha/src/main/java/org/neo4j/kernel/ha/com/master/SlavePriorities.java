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
package org.neo4j.kernel.ha.com.master;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.PrefetchingIterator;

import static java.util.Collections.reverseOrder;

/**
 * Factory for common {@link SlavePriority} implementations.
 * 
 * @author Mattias Persson
 */
public abstract class SlavePriorities
{
    // Purely a factory.
    private SlavePriorities()
    {
    }

    /**
     * @return {@link SlavePriority} which returns the slaves in the order that
     * they are given in the {@code slaves} array.
     */
    public static SlavePriority givenOrder()
    {
        return new SlavePriority()
        {
            @Override
            public Iterable<Slave> prioritize( Iterable<Slave> slaves )
            {
                return slaves;
            }
        };
    }
    
    /**
     * @return {@link SlavePriority} which returns the slaves in a round robin
     * fashion, more precisely the start index in the array increments with
     * each {@link SlavePriority#prioritize(Iterable) prioritization}, ordered
     * by server id in ascending.
     */
    public static SlavePriority roundRobin()
    {
        return new SlavePriority()
        {
            final AtomicInteger index = new AtomicInteger();
            
            @Override
            public Iterable<Slave> prioritize( final Iterable<Slave> slaves )
            {
                final List<Slave> slaveList = sortSlaves( slaves, true );
                if ( slaveList.isEmpty() )
                {
                    return Iterables.empty();
                }
                return new Iterable<Slave>()
                {
                    @Override
                    public Iterator<Slave> iterator()
                    {
                        return new PrefetchingIterator<Slave>()
                        {
                            private int start = index.getAndIncrement()%slaveList.size();
                            private int count;

                            @Override
                            protected Slave fetchNextOrNull()
                            {
                                int id = count++;
                                return id <= slaveList.size() ? slaveList.get( (start+id)%slaveList.size() ) : null;
                            }
                        };
                    }
                };
            }
        };
    }
    
    /**
     * @return {@link SlavePriority} which returns the slaves in the same fixed order
     * sorted by server id in descending order.
     */
    public static SlavePriority fixedDescending()
    {
        return new SlavePriority()
        {
            @Override
            public Iterable<Slave> prioritize( final Iterable<Slave> slaves )
            {
                return sortSlaves( slaves, false );
            }
        };
    }

    /**
     * @return {@link SlavePriority} which returns the slaves in the same fixed order
     * sorted by server id in ascending order. This is generally preferrable to {@link #fixedDescending()},
     * because this aligns with the tie-breaker aspect of the lowest server id becoming master.
     * <p>
     * Eg. if you want to ensure that failover most likely will happen in a specific datacenter,
     * you would place low-id instances in that datacenter and choose this strategy. That way,
     * most of the time the most up-to-date instance will be in this data center, and if there is
     * a tie, the tie-break will also end up electing a master in the same data center.
     * <p>
     * This is in contrast to {@link #fixedDescending()}, where a high-id server is likely to be most
     * up-to-date if the master fails, but a low-id server is likely to be elected if there is a tie.
     * This makes the two scenarios consistently choose a low-id server as the new master.
     */
    public static SlavePriority fixedAscending()
    {
        return new SlavePriority()
        {
            @Override
            public Iterable<Slave> prioritize( final Iterable<Slave> slaves )
            {
                return sortSlaves( slaves, true );
            }
        };
    }

    private static List<Slave> sortSlaves( final Iterable<Slave> slaves, boolean asc )
    {
        ArrayList<Slave> slaveList = Iterables.addAll( new ArrayList<Slave>(), slaves );
        Collections.sort( slaveList, asc ? SERVER_ID_COMPARATOR : REVERSE_SERVER_ID_COMPARATOR );
        return slaveList;
    }

    private static final Comparator<Slave> SERVER_ID_COMPARATOR = new Comparator<Slave>()
    {
        public int compare( Slave first, Slave second )
        {
            return first.getServerId() - second.getServerId();
        }
    };
    
    private static final Comparator<Slave> REVERSE_SERVER_ID_COMPARATOR = reverseOrder( SERVER_ID_COMPARATOR );
}
