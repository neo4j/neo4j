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
package org.neo4j.kernel.ha;

import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.Logger;

/**
 * TODO
 */
public class HighAvailabilityDiagnostics
        implements DiagnosticsProvider
{
    private final HighAvailabilityMemberStateMachine memberStateMachine;
    private final ClusterClient clusterClient;

    public HighAvailabilityDiagnostics( HighAvailabilityMemberStateMachine memberStateMachine,
                                        ClusterClient clusterClient )
    {
        this.memberStateMachine = memberStateMachine;
        this.clusterClient = clusterClient;
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
    public void dump( DiagnosticsPhase phase, Logger logger )
    {
        StringBuilder builder = new StringBuilder();

        builder.append( "High Availability diagnostics\n" ).
                append( "Member state:" ).append( memberStateMachine.getCurrentState().name() ).append( "\n" ).
                append( "State machines:\n" );
        
        clusterClient.dumpDiagnostics( builder );
        logger.log( builder.toString() );
    }
}
