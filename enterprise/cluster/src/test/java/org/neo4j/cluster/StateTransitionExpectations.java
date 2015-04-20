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
package org.neo4j.cluster;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.cluster.statemachine.StateTransition;
import org.neo4j.cluster.statemachine.StateTransitionListener;

public class StateTransitionExpectations<CONTEXT,MESSAGETYPE extends Enum<MESSAGETYPE> & MessageType>
{
    public final StateTransitionListener NO_EXPECTATIONS = new StateTransitionListener()
    {
        @Override
        public void stateTransition( StateTransition messagetypeStateTransition )
        {
        }
    };
    
    private final List<ExpectingStateTransitionListener> expectations = new ArrayList<ExpectingStateTransitionListener>();
    
    public ExpectationsBuilder newExpectations( Enum<?>... initialAlternatingExpectedMessageAndState )
    {
        ExpectationsBuilder builder = new ExpectationsBuilder();
        for ( int i = 0; i < initialAlternatingExpectedMessageAndState.length; i++ )
            builder.expect( (MessageType) initialAlternatingExpectedMessageAndState[i++], (State) initialAlternatingExpectedMessageAndState[i] );
        return builder;
    }
    
    public void verify()
    {
        StringBuilder builder = new StringBuilder(  );
        for ( ExpectingStateTransitionListener listener : expectations )
            listener.transitionHistory( builder );
        
        if (builder.length() != 0)
        {
            throw new IllegalStateException( "Failed expectations:"+builder.toString() );
        }
    }
    
    public boolean areFulfilled()
    {
        for ( ExpectingStateTransitionListener listener : expectations )
            if ( !listener.isFulfilled() )
                return false;
        return true;
    }

    public void printRemaining( Logger logger )
    {
        StringBuilder builder = new StringBuilder(  );
        builder.append( "=== Remaining state transitions ===\n" );
        for( ExpectingStateTransitionListener expectation : expectations )
        {
            expectation.printRemaining(builder);
        }
        logger.info( builder.toString() );
    }

    public class ExpectationsBuilder
    {
        private final Deque<ExpectedTransition> transitions = new LinkedList<ExpectedTransition>();
        private boolean includeUnchanged;
        
        public ExpectationsBuilder expect( MessageType messageToGetHere, State state )
        {
            transitions.add( new ExpectedTransition( messageToGetHere, state ) );
            return this;
        }
        
        public ExpectationsBuilder includeUnchangedStates()
        {
            this.includeUnchanged = true;
            return this;
        }
        
        public StateTransitionListener build( Object id )
        {
            ExpectingStateTransitionListener listener = new ExpectingStateTransitionListener( new LinkedList<ExpectedTransition>( transitions ), includeUnchanged, id );
            expectations.add( listener );
            return listener;
        }
        
        public void assertNoMoreExpectations()
        {
            if ( !transitions.isEmpty() )
                throw new IllegalStateException( "Unsatisfied transitions: " + transitions );
        }
    }
    
    private class ExpectingStateTransitionListener implements StateTransitionListener
    {
        private final Deque<String> transitionHistory = new LinkedList<String>();
        private final Deque<ExpectedTransition> transitions;
        private volatile boolean valid = true;
        private final Object id;
        private final boolean includeUnchanged;

        ExpectingStateTransitionListener( Deque<ExpectedTransition> transitions, boolean includeUnchanged, Object id )
        {
            this.transitions = transitions;
            this.includeUnchanged = includeUnchanged;
            this.id = id;
        }

        @Override
        public void stateTransition( StateTransition transition )
        {
            if (valid || transitions.isEmpty())
            {
                if ( !includeUnchanged && transition.getOldState().equals( transition.getNewState() ) )
                    return;
    
                if ( transitions.isEmpty())
                {
                    valid = false;
                    transitionHistory.add( "UNEXPECTED:" + transition );
                } else
                {
                    ExpectedTransition expected = transitions.pop();
                    if ( expected.matches( transition ) )
                    {
                        transitionHistory.add( expected.toString() );
                    } else
                    {
                        transitionHistory.add( "EXPECTED " + expected + ", GOT " + transition );
                        valid = false;
                    }
                }
            }
        }
        
        void transitionHistory(StringBuilder builder)
        {
            if (valid && transitions.isEmpty())
                return;

            builder.append( "\n=== Failed state transition expectations for " ).append( id );
            for( String transition : transitionHistory )
            {
                builder.append( "\n " ).append( transition );
            }

            if (valid)
            {
                for( ExpectedTransition transition : transitions )
                {
                    builder.append( "\n " ).append( "MISSING ").append( transition );
                }
            }
        }

        void printRemaining( StringBuilder builder )
        {
            builder.append( "==  " ).append( id ).append( "\n" );
            for( ExpectedTransition transition : transitions )
            {
                builder.append( transition.toString() ).append( "\n" );
            }
        }
        
        public boolean isFulfilled()
        {
            return valid && transitions.isEmpty();
        }
    }
    
    private class ExpectedTransition
    {
        private final MessageType messageToGetHere;
        private final State state;
        
        ExpectedTransition( MessageType messageToGetHere, State state )
        {
            this.messageToGetHere = messageToGetHere;
            this.state = state;
        }

        public boolean matches( StateTransition transition )
        {
            return state.equals( transition.getNewState() ) && messageToGetHere.equals( transition.getMessage().getMessageType() );
        }
        
        @Override
        public String toString()
        {
            return messageToGetHere + "->" + state;
        }
    }
}
