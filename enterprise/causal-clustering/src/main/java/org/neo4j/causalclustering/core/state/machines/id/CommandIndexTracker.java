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
package org.neo4j.causalclustering.core.state.machines.id;

/**
 * Keeps track of the raft command index of last applied transaction.
 *
 * As soon as a transaction is successfully applied this will be updated to reflect that.
 */
public class CommandIndexTracker
{
    private volatile long appliedCommandIndex;

    public void setAppliedCommandIndex( long appliedCommandIndex )
    {
        this.appliedCommandIndex = appliedCommandIndex;
    }

    long getAppliedCommandIndex()
    {
        return appliedCommandIndex;
    }
}
