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
package org.neo4j.causalclustering.cluster_load;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.helpers.DataCreator;

public class SmallBurst implements ClusterLoad
{
    @Override
    public void start( Cluster cluster ) throws Exception
    {
        DataCreator.createEmptyNodes( cluster, 10 );
    }

    @Override
    public void stop()
    {
        // do nothing
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
