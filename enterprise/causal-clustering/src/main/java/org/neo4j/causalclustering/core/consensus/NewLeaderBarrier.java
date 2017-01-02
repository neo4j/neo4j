/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
