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
package org.neo4j.causalclustering.stresstests;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.helper.Workload;

abstract class RepeatOnRandomCore extends Workload
{
    private final Cluster cluster;

    RepeatOnRandomCore( Control control, Resources resources )
    {
        super( control );
        this.cluster = resources.cluster();
    }

    @Override
    protected final void doWork() throws Exception
    {
        doWorkOnMember( cluster.randomCoreMember( true ).orElseThrow( IllegalStateException::new ) );
    }

    protected abstract void doWorkOnMember( ClusterMember member ) throws Exception;
}
