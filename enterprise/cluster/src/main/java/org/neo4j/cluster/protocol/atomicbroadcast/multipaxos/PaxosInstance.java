/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Record of an individual Paxos instance, from a proposer perspective
 */
public class PaxosInstance
{
    enum State
    {
        empty,
        p1_pending,
        p1_ready,
        p2_pending,
        closed,
        delivered
    }

    PaxosInstanceStore store;
    InstanceId id = null;
    State state = State.empty;
    long ballot = 0;
    List<URI> acceptors;
    List<ProposerMessage.PromiseState> promises = new ArrayList<ProposerMessage.PromiseState>();
    List<ProposerMessage.AcceptedState> accepts = new ArrayList<ProposerMessage.AcceptedState>();
    List<ProposerMessage.RejectAcceptState> rejectedAccepts = new ArrayList<ProposerMessage.RejectAcceptState>();
    Object value_1;
    long phase1Ballot = 0;
    Object value_2;
    boolean clientValue = false;
    String conversationIdHeader;

    public PaxosInstance( PaxosInstanceStore store, InstanceId instanceId )
    {
        this.store = store;
        this.id = instanceId;
    }

    public boolean isState( State s )
    {
        return state.equals( s );
    }

    public void propose( long ballot, List<URI> acceptors )
    {
        this.state = State.p1_pending;
        this.acceptors = acceptors;
        this.ballot = ballot;
    }

    public void phase1Timeout( long ballot, List<URI> newAcceptors )
    {
        this.ballot = ballot;
        promises.clear();
        this.acceptors = newAcceptors;
    }

    public void promise( ProposerMessage.PromiseState promiseState )
    {
        promises.add( promiseState );
        if ( promiseState.getValue() != null && promiseState.getBallot() > phase1Ballot )
        {
            value_1 = promiseState.getValue();
            phase1Ballot = promiseState.getBallot();
        }
    }

    public boolean isPromised( int minimumQuorumSize )
    {
        return promises.size() == minimumQuorumSize;
    }

    public void ready( Object value, boolean clientValue )
    {
        state = State.p1_ready;
        promises.clear();
        value_1 = null;
        phase1Ballot = 0;
        value_2 = value;
        this.clientValue = clientValue;
    }

    public void pending()
    {
        state = State.p2_pending;
    }

    public void phase2Timeout( long ballot )
    {
        state = State.p1_pending;
        this.ballot = ballot;
        promises.clear();
        value_1 = null;
        phase1Ballot = 0;
    }

    public void accepted( ProposerMessage.AcceptedState acceptedState )
    {
        accepts.add( acceptedState );
    }

    public void rejected( ProposerMessage.RejectAcceptState rejectAcceptState )
    {
        rejectedAccepts.add( rejectAcceptState );
    }

    public boolean isAccepted( int minimumQuorumSize )
    {
        // If we have received enough responses to meet quorum and a majority
        // are accepts, then the instance is considered accepted
        return accepts.size() + rejectedAccepts.size() == minimumQuorumSize &&
                accepts.size() > rejectedAccepts.size();
    }

    public void closed( Object value, String conversationIdHeader )
    {
        value_2 = value;
        state = State.closed;
        accepts.clear();
        rejectedAccepts.clear();
        acceptors = null;
        this.conversationIdHeader = conversationIdHeader;
    }

    public void delivered()
    {
        state = State.delivered;
        store.delivered( id );
    }

    public List<URI> getAcceptors()
    {
        return acceptors;
    }

    @Override
    public String toString()
    {
        return id + ": " + state.name() + " b=" + ballot;
    }
}
