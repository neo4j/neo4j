/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.net.Outbound;
import org.neo4j.kernel.impl.store.StoreId;


public class LeaderOnlyReplicator<MEMBER,SOCKET>
{
    private final MEMBER source;
    private final SOCKET target;
    private final Outbound<SOCKET> outbound;

    public LeaderOnlyReplicator( MEMBER source, SOCKET target, Outbound<SOCKET> outbound )
    {
        this.source = source;
        this.target = target;
        this.outbound = outbound;
    }

    public void replicate( ReplicatedContent content, StoreId storeId )
    {
        outbound.send( target, new RaftMessages.NewEntry.Request<>( source, content, storeId ) );
    }
}
