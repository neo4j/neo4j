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

import java.io.Serializable;

import org.neo4j.causalclustering.identity.MemberId;

public class LeaderInfo implements Serializable
{

    private static final long serialVersionUID = 7983780359510842910L;

    public static final LeaderInfo INITIAL = new LeaderInfo( null, -1 );

    private final MemberId memberId;
    private final long term;
    private boolean isSteppingDown;

    public LeaderInfo( MemberId memberId, long term )
    {
        this( memberId, term, false );
    }

    private LeaderInfo( MemberId memberId, long term, boolean isSteppingDown )
    {
        this.memberId = memberId;
        this.term = term;
        this.isSteppingDown = isSteppingDown;
    }

    /**
     * Produces a new LeaderInfo object for a step down event, setting memberId to null but maintaining the current term.
     */
    public LeaderInfo stepDown()
    {
        return new LeaderInfo( null, this.term, true );
    }

    public boolean isSteppingDown()
    {
        return isSteppingDown;
    }

    public MemberId memberId()
    {
        return memberId;
    }

    public long term()
    {
        return term;
    }

    @Override
    public String toString()
    {
        return "LeaderInfo{" + "memberId=" + memberId + ", term=" + term + '}';
    }
}
