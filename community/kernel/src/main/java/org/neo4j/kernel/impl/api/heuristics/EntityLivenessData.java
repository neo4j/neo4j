/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.heuristics;

import org.neo4j.kernel.impl.util.statistics.RollingAverage;

import java.io.Serializable;

/**
 * Track percentage of live entities vs dead (deleted or corrupt) entity (records).
 */
public final class EntityLivenessData implements Serializable {

    private static final long serialVersionUID = -8657743503560688270L;

    private final RollingAverage liveEntities = new RollingAverage( HeuristicsData.WINDOW_SIZE );
    private final RollingAverage deadEntities = new RollingAverage( HeuristicsData.WINDOW_SIZE );
    private long maxEntities = 0;

    transient private int liveEntitiesSeenInRound;
    transient private int deadEntitiesSeenInRound;

    public EntityLivenessData() {
    }

    private void reset()
    {
        /* This avoids division by zero and ensures a result of 1.0 for an empty database, cf. liveEntitiesRatio() */
        liveEntitiesSeenInRound = 1;
        deadEntitiesSeenInRound = 0;
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
        reset();

    }

    /**
     * @return fraction of live nodes (value between 0.0 and 1.0) seen by sampling
     */
    public double liveEntitiesRatio()
    {
        double alive = liveEntities.average();
        double dead = deadEntities.average();
        return alive / (dead + alive);
    }

    public long maxAddressableEntities()
    {
        return maxEntities;
    }

    public void setMaxEntities(long maxEntities)
    {
        this.maxEntities = maxEntities;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        EntityLivenessData that = (EntityLivenessData) o;

        return
            liveEntitiesSeenInRound == that.liveEntitiesSeenInRound
            && maxEntities == that.maxEntities
            && deadEntitiesSeenInRound == that.deadEntitiesSeenInRound
            && deadEntities.equals(that.deadEntities)
            && liveEntities.equals(that.liveEntities);
    }

    @Override
    public int hashCode() {
        int result = liveEntities.hashCode();
        result = 31 * result + deadEntities.hashCode();
        result = 31 * result + (int) (maxEntities ^ (maxEntities >>> 32));
        result = 31 * result + liveEntitiesSeenInRound;
        result = 31 * result + deadEntitiesSeenInRound;
        return result;
    }
}
