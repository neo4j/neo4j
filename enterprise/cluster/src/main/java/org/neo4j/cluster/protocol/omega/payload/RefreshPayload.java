/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.omega.payload;

import java.io.Serializable;

import org.neo4j.helpers.Pair;
import org.neo4j.cluster.protocol.omega.state.EpochNumber;
import org.neo4j.cluster.protocol.omega.state.State;

public final class RefreshPayload implements Serializable
{
    public final int serialNum;
    public final int processId;
    public final int freshness;
    public final int refreshRound;

    public RefreshPayload( int serialNum, int processId, int freshness, int refreshRound )
    {
        this.serialNum = serialNum;
        this.processId = processId;
        this.freshness = freshness;
        this.refreshRound = refreshRound;
    }

    public static RefreshPayload fromState( State state, int refreshRound )
    {
        return new RefreshPayload( state.getEpochNum().getSerialNum(), state.getEpochNum().getProcessId(),
                state.getFreshness(), refreshRound );
    }

    public static Pair<Integer, State> toState( RefreshPayload payload )
    {
        EpochNumber epochNumber = new EpochNumber( payload.serialNum, payload.processId );
        State result = new State( epochNumber, payload.freshness );
        return Pair.of( payload.refreshRound, result );
    }

    @Override
    public String toString()
    {
        return "RefreshPayload[serialNum= " + serialNum + ", processId=" + processId + ", " +
                "freshness=" + freshness + ", refreshRound=" + refreshRound + "]";
    }
}
