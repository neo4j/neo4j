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
 * Track percentage of live nodes vs dead (deleted or corrupt) node (records).
 */
public final class NodeLivenessTracker implements Serializable {

    private static final long serialVersionUID = -8657743503560688270L;

    private final RollingAverage liveNodes = new RollingAverage( HeuristicsData.WINDOW_SIZE );
    private final RollingAverage deadNodes = new RollingAverage( HeuristicsData.WINDOW_SIZE );
    private long maxNodes = 0;
    private long round = 0;

    transient private int liveNodesSeenNow;
    transient private int skippedNodesSeenNow;

    public NodeLivenessTracker() {
    }

    private void reset()
    {
        /* This avoids division by zero and ensures a result of 1.0 for an empty database, cf. liveNodesRatio() */
        liveNodesSeenNow = 1;
        skippedNodesSeenNow = 0;
        round++;
    }

    /**
     * Record a sampled node as being alive (i.e. part of the database state)
     */
    public void recordLiveNode()
    {
        liveNodesSeenNow++;
    }

    /**
     * Record a sampled node as being dead (i.e. either deleted or corrupt)
     */
    public void recordDeadNode()
    {
        skippedNodesSeenNow++;
    }

    /**
     * Record live and dead nodes seen so far in the current sampling round and start
     * a new sampling round
     */
    public void recalculate()
    {
        liveNodes.record( liveNodesSeenNow );
        deadNodes.record( skippedNodesSeenNow );
        reset();

    }

    /**
     * @return fraction of live nodes (value between 0.0 and 1.0) seen by sampling
     */
    public double liveNodes()
    {
        if ( round > 0 ) {
            double alive = liveNodes.average();
            double dead = deadNodes.average();
            return alive / (dead + alive);
        }
        else
        {
            return 1.0;
        }
    }

    public long maxNodes()
    {
        return maxNodes;
    }

    public void setMaxNodes( long maxNodes )
    {
        this.maxNodes = maxNodes;
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

        NodeLivenessTracker that = (NodeLivenessTracker) o;

        return
            liveNodesSeenNow == that.liveNodesSeenNow
            && maxNodes == that.maxNodes
            && round == that.round
            && skippedNodesSeenNow == that.skippedNodesSeenNow
            && deadNodes.equals(that.deadNodes)
            && liveNodes.equals(that.liveNodes);
    }

    @Override
    public int hashCode() {
        int result = liveNodes.hashCode();
        result = 31 * result + deadNodes.hashCode();
        result = 31 * result + (int) (maxNodes ^ (maxNodes >>> 32));
        result = 31 * result + (int) (round ^ (round >>> 32));
        result = 31 * result + liveNodesSeenNow;
        result = 31 * result + skippedNodesSeenNow;
        return result;
    }
}
