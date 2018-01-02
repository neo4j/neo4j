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
package org.neo4j.cluster.protocol.snapshot;

import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

/**
 * TODO
 */
public class SnapshotContext
{
    private SnapshotProvider snapshotProvider;
    private ClusterContext clusterContext;
    private LearnerContext learnerContext;

    public SnapshotContext( ClusterContext clusterContext, LearnerContext learnerContext )
    {
        this.clusterContext = clusterContext;
        this.learnerContext = learnerContext;
    }

    public void setSnapshotProvider( SnapshotProvider snapshotProvider)
    {
        this.snapshotProvider = snapshotProvider;
    }

    public ClusterContext getClusterContext()
    {
        return clusterContext;
    }

    public LearnerContext getLearnerContext()
    {
        return learnerContext;
    }



    public SnapshotProvider getSnapshotProvider()
    {
        return snapshotProvider;
    }
}
