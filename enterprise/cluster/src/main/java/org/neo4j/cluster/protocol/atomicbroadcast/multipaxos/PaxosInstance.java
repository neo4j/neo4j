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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;

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
        delivered;
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
    // This is true iff the acceptors did not already have a value for this instance
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

    public long getBallot()
    {
        return ballot;
    }

    public void propose( long ballot, List<URI> acceptors )
    {
        this.state = State.p1_pending;
        this.acceptors = acceptors;
        this.ballot = ballot;
    }

    public void phase1Timeout( long ballot )
    {
        this.ballot = ballot;
        promises.clear();
    }

    public void promise( ProposerMessage.PromiseState promiseState )
    {
        promises.add( promiseState );
        if ( promiseState.getValue() != null && promiseState.getBallot() >= phase1Ballot )
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
        assertNotNull( value );
        
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
        assertNotNull( value );
        
        value_2 = value;
        state = State.closed;
        accepts.clear();
        rejectedAccepts.clear();
        acceptors = null;
        this.conversationIdHeader = conversationIdHeader;
    }

    private void assertNotNull( Object value )
    {
        if ( value == null )
        {
            throw new IllegalArgumentException( "value null" );
        }
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

    public PaxosInstance snapshot(PaxosInstanceStore store)
    {
        PaxosInstance snap = new PaxosInstance( store, id );

        snap.state = state;
        snap.ballot = ballot;
        snap.acceptors = acceptors == null ? null : new ArrayList<>(acceptors);
        snap.promises = promises == null ? null : new ArrayList<>(promises);
        snap.accepts = accepts == null ? null : new ArrayList<>(accepts);
        snap.rejectedAccepts = rejectedAccepts == null ? null : new ArrayList<>(rejectedAccepts);
        snap.value_1 = value_1;
        snap.phase1Ballot = phase1Ballot;
        snap.value_2 = value_2;
        snap.clientValue = clientValue;
        snap.conversationIdHeader = conversationIdHeader;

        return snap;
    }

    @Override
    public String toString()
    {
        try
        {
            Object toStringValue1 = null;
            if (value_1 != null)
            {
                if ( value_1 instanceof Payload )
                {
                    toStringValue1 = new AtomicBroadcastSerializer().receive( (Payload) value_1 ).toString();
                }
                else
                {
                    toStringValue1 = value_1.toString();
                }
            }

            Object toStringValue2 = null;
            if (value_2 != null)
            {
                if ( value_2 instanceof Payload )
                {
                    toStringValue2 = new AtomicBroadcastSerializer().receive( (Payload) value_2 ).toString();
                }
                else
                {
                    toStringValue2 = value_2.toString();
                }
            }

            return "[id:" + id +
                   " state:" + state.name() +
                   " b:" + ballot +
                   " v1:" + toStringValue1 +
                   " v2:" + toStringValue2 + "]";
        }
        catch ( Throwable e )
        {
            return "";
        }
    }
}
