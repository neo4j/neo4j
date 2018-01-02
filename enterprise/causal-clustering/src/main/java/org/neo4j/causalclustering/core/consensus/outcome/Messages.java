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
package org.neo4j.causalclustering.core.consensus.outcome;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.MemberId;

public class Messages implements Iterable<Map.Entry<MemberId, RaftMessages.RaftMessage>>
{
    private final Map<MemberId, RaftMessages.RaftMessage> map;

    Messages( Map<MemberId, RaftMessages.RaftMessage> map )
    {
        this.map = map;
    }

    public boolean hasMessageFor( MemberId member )
    {
        return map.containsKey( member );
    }

    public RaftMessages.RaftMessage messageFor( MemberId member )
    {
        return map.get( member );
    }

    @Override
    public Iterator<Map.Entry<MemberId, RaftMessages.RaftMessage>> iterator()
    {
        return map.entrySet().iterator();
    }
}
