/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package org.neo4j.kernel.ha;

import java.util.Map;

import org.neo4j.cluster.ConnectedStateMachines;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.ha.cluster.ClusterMemberStateMachine;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;

/**
 * TODO
 */
public class HighAvailabilityDiagnostics
        implements DiagnosticsProvider
{
    private ClusterMemberStateMachine memberStateMachine;
    private ConnectedStateMachines stateMachines;

    public HighAvailabilityDiagnostics( ClusterMemberStateMachine memberStateMachine,
                                        ConnectedStateMachines stateMachines )
    {
        this.memberStateMachine = memberStateMachine;
        this.stateMachines = stateMachines;
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return getClass().getSimpleName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
    }

    @Override
    public void dump( DiagnosticsPhase phase, StringLogger log )
    {
        StringBuilder builder = new StringBuilder();

        builder.append( "High Availability diagnostics\n" ).
                append( "Member state:" ).append( memberStateMachine.getCurrentState().name() ).append( "\n" ).
                append( "State machines:\n" );

        for ( StateMachine stateMachine : stateMachines.getStateMachines() )
        {
            builder.append( "   " ).append( stateMachine.getMessageType().getSimpleName() ).append( ":" ).append(
                    stateMachine.getState().toString() ).append( "\n" );
        }

        builder.append( "Current timeouts:\n" );
        for ( Map.Entry<Object, Timeouts.Timeout> objectTimeoutEntry : stateMachines.getTimeouts().getTimeouts()
                .entrySet() )
        {
            builder.append( objectTimeoutEntry.getKey().toString() ).append( ":" ).append( objectTimeoutEntry
                    .getValue().getTimeoutMessage().toString() );
        }

        log.logMessage( builder.toString() );
    }
}
