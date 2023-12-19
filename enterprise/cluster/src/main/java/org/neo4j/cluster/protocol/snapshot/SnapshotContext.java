/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.snapshot;

import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

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

    public void setSnapshotProvider( SnapshotProvider snapshotProvider )
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
