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
package org.neo4j.coreedge.raft;

public class Ballot
{
    public static <MEMBER> boolean shouldVoteFor( MEMBER candidate, long requestTerm, long contextTerm,
                                                  long contextLastAppended, long requestLastLogIndex,
                                                  long contextLastLogTerm, long requestLastLogTerm,
                                                  MEMBER votedFor )
    {
        if ( requestTerm < contextTerm )
        {
            return false;
        }

        boolean termOk = contextLastLogTerm <= requestLastLogTerm;
        boolean appendedOk = contextLastAppended <= requestLastLogIndex;
        boolean requesterLogUpToDate = termOk && appendedOk;
        boolean votedForOtherInSameTerm = (requestTerm == contextTerm &&
                !(votedFor == null || votedFor.equals( candidate )));

        return requesterLogUpToDate && !votedForOtherInSameTerm;
    }
}
