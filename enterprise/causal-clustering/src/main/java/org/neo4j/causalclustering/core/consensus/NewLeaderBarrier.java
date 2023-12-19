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
package org.neo4j.causalclustering.core.consensus;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;

/**
 * When a new leader is elected, it replicates one entry of this type to mark the start of its reign.
 * By listening for content of this type, we can partition content by leader reign.
 */
public class NewLeaderBarrier implements ReplicatedContent
{
    @Override
    public String toString()
    {
        return "NewLeaderBarrier";
    }

    @Override
    public int hashCode()
    {
        return 1;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof NewLeaderBarrier;
    }
}
