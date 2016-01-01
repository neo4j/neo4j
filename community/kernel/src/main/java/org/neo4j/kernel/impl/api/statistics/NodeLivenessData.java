/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.statistics;

import org.neo4j.kernel.impl.util.statistics.RollingAverage;

import java.io.Serializable;

/**
 * Track percentage of live entities vs dead (deleted or corrupt) entity (records).
 */
public final class NodeLivenessData implements Serializable {

    private static final long serialVersionUID = -8657743503560688270L;

    private final RollingAverage liveEntities;
    private final RollingAverage deadEntities;
    private long highestNodeId = 0;

    transient private int liveEntitiesSeenInRound;
    transient private int deadEntitiesSeenInRound;

    public NodeLivenessData(RollingAverage.Parameters parameters)
    {
        this.liveEntities = new RollingAverage( parameters );
        this.deadEntities = new RollingAverage( parameters );
    }

    /**
     * Record a sampled node as being alive (i.e. part of the database state)
     */
    public void recordLiveEntity()
    {
        liveEntitiesSeenInRound++;
    }

    /**
     * Record a sampled node as being dead (i.e. either deleted or corrupt)
     */
    public void recordDeadEntity()
    {
        deadEntitiesSeenInRound++;
    }

    /**
     * Record live and dead nodes seen so far in the current sampling round and start
     * a new sampling round
     */
    public void recalculate()
    {
        liveEntities.record( liveEntitiesSeenInRound );
        deadEntities.record( deadEntitiesSeenInRound );

        liveEntitiesSeenInRound = 0;
        deadEntitiesSeenInRound = 0;
    }

    /**
     * @return fraction of live nodes (value between 0.0 and 1.0) seen by sampling
     */
    public double liveEntitiesRatio()
    {
        double alive = liveEntities.average();
        double total = deadEntities.average() + alive;
        return total <= 0 ? 1.0 : alive / total;
    }

    public long highestNodeId()
    {
        return highestNodeId;
    }

    public void recordHighestId(long nodeId)
    {
        this.highestNodeId = nodeId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        NodeLivenessData that = (NodeLivenessData) o;

        return
            highestNodeId == that.highestNodeId
            && deadEntities.equals( that.deadEntities )
            && liveEntities.equals( that.liveEntities );

    }

    @Override
    public int hashCode() {
        int result = liveEntities.hashCode();
        result = 31 * result + deadEntities.hashCode();
        result = 31 * result + (int) ( highestNodeId ^ ( highestNodeId >>> 32 ) );
        return result;
    }
}
