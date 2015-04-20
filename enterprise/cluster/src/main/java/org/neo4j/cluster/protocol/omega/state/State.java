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
package org.neo4j.cluster.protocol.omega.state;

public class State implements Comparable<State>
{
    private final EpochNumber epochNum;
    private int freshness;

    public State( EpochNumber epochNum, int freshness )
    {
        this.epochNum = epochNum;
        this.freshness = freshness;
    }

    public State( EpochNumber epochNum )
    {
        this( epochNum, 0 );
    }

    @Override
    public int compareTo( State o )
    {
        return epochNum.compareTo( o.epochNum ) == 0 ? freshness - o.freshness : epochNum.compareTo( o.epochNum );
    }

    public EpochNumber getEpochNum()
    {
        return epochNum;
    }

    public int getFreshness()
    {
        return freshness;
    }

    public int increaseFreshness()
    {
        return freshness++;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null )
        {
            return false;
        }
        if ( !(obj instanceof State) )
        {
            return false;
        }
        State other = (State) obj;
        return epochNum.equals( other.epochNum ) && (freshness == other.freshness);
    }

    @Override
    public String toString()
    {
        return "State [Epoch: "+epochNum+", freshness= "+freshness+"]";
    }
}
