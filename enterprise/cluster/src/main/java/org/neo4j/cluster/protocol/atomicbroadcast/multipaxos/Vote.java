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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.election.ElectionCredentials;
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
    private final ElectionCredentials voteCredentials;

    public Vote( InstanceId suggestedNode, ElectionCredentials voteCredentials )
    {
        this.suggestedNode = suggestedNode;
        this.voteCredentials = voteCredentials;
    }

    public org.neo4j.cluster.InstanceId getSuggestedNode()
    {
        return suggestedNode;
    }

    public Comparable<ElectionCredentials> getCredentials()
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
        return voteCredentials.equals( vote.voteCredentials );
    }

    @Override
    public int hashCode()
    {
        int result = suggestedNode.hashCode();
        result = 31 * result + voteCredentials.hashCode();
        return result;
    }
}
