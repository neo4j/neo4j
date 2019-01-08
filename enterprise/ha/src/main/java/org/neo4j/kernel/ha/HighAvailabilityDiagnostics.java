/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
