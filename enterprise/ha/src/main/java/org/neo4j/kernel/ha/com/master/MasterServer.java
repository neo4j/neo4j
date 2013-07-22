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
package org.neo4j.kernel.ha.com.master;

import org.jboss.netty.channel.Channel;
import org.neo4j.com.*;
import org.neo4j.kernel.ha.HaRequestType20;
import org.neo4j.kernel.ha.MasterClient20;
import org.neo4j.kernel.ha.transaction.UnableToResumeTransactionException;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.tooling.RealClock;

import java.io.IOException;
import java.util.*;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link org.neo4j.kernel.ha.com.slave.MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer extends Server<Master, Void>
{
    public static final int FRAME_LENGTH = Protocol.DEFAULT_FRAME_LENGTH;

    public MasterServer( Master requestTarget, Logging logging, Configuration config,
                         TxChecksumVerifier txVerifier ) throws IOException
    {
        super( requestTarget, config, logging, FRAME_LENGTH, MasterClient20.PROTOCOL_VERSION, txVerifier,
                new RealClock() );
    }

    @Override
    protected RequestType<Master> getRequestContext( byte id )
    {
        return HaRequestType20.values()[id];
    }

    @Override
    protected void finishOffChannel( Channel channel, RequestContext context )
    {
        getRequestTarget().finishTransaction( context, false );
    }

    @Override
    protected boolean shouldLogFailureToFinishOffChannel( Throwable failure )
    {
        return !(failure instanceof UnableToResumeTransactionException);
    }

    public Map<Integer, Collection<RequestContext>> getSlaveInformation()
    {
        // Which slaves are connected a.t.m?
        Set<Integer> machineIds = new HashSet<Integer>();
        Map<Channel, RequestContext> channels = getConnectedSlaveChannels();
        synchronized ( channels )
        {
            for ( RequestContext context : channels.values() )
            {
                machineIds.add( context.machineId() );
            }
        }

        // Insert missing slaves into the map so that all connected slave
        // are in the returned map
        Map<Integer, Collection<RequestContext>> ongoingTransactions =
                ((MasterImpl) getRequestTarget()).getOngoingTransactions();
        for ( Integer machineId : machineIds )
        {
            if ( !ongoingTransactions.containsKey( machineId ) )
            {
                ongoingTransactions.put( machineId, Collections.<RequestContext>emptyList() );
            }
        }
        return new TreeMap<Integer, Collection<RequestContext>>( ongoingTransactions );
    }
}
