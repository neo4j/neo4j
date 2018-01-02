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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;

/**
 * A cluster coordinator election vote. Each vote contains the id of the server and any credentials (see {@link
 * ElectionCredentialsProvider} implementations for details).
 * <p/>
 * Votes are comparable so that they can be ordered to find winner. Credentials implement the comparison rules.
 */
public class Vote
        implements Comparable<Vote>
{
    private final InstanceId suggestedNode;
    private final Comparable<Object> voteCredentials;

    public Vote( InstanceId suggestedNode, Comparable<Object> voteCredentials )
    {
        this.suggestedNode = suggestedNode;
        this.voteCredentials = voteCredentials;
    }

    public org.neo4j.cluster.InstanceId getSuggestedNode()
    {
        return suggestedNode;
    }

    public Comparable<Object> getCredentials()
    {
        return voteCredentials;
    }

    @Override
    public String toString()
    {
        return suggestedNode + ":" + voteCredentials;
    }

    @Override
    public int compareTo( Vote o )
    {
        return this.voteCredentials.compareTo( o.voteCredentials );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Vote vote = (Vote) o;

        if ( !suggestedNode.equals( vote.suggestedNode ) )
        {
            return false;
        }
        if ( !voteCredentials.equals( vote.voteCredentials ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = suggestedNode.hashCode();
        result = 31 * result + voteCredentials.hashCode();
        return result;
    }
}
