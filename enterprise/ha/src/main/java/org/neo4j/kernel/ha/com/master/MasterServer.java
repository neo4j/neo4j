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
package org.neo4j.kernel.ha.com.master;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jboss.netty.channel.Channel;

import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Response;
import org.neo4j.com.Server;
import org.neo4j.com.TransactionNotPresentOnMasterException;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.kernel.ha.HaRequestType210;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link org.neo4j.kernel.ha.com.slave.MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer extends Server<Master, Void>
{
    public static final int FRAME_LENGTH = Protocol.DEFAULT_FRAME_LENGTH;

    public MasterServer( Master requestTarget, Logging logging, Configuration config,
                         TxChecksumVerifier txVerifier, ByteCounterMonitor byteCounterMonitor,
                         RequestMonitor requestMonitor )
    {
        super( requestTarget, config, logging, FRAME_LENGTH, MasterClient214.PROTOCOL_VERSION, txVerifier,
                SYSTEM_CLOCK, byteCounterMonitor, requestMonitor );
    }

    @Override
    protected RequestType<Master> getRequestContext( byte id )
    {
        return HaRequestType210.values()[id];
    }

    @Override
    protected void finishOffChannel( Channel channel, RequestContext context )
    {
        try
        {
            try ( Response<Void> ignored = getRequestTarget().endLockSession( context, false ) )
            {
                // Lock session is closed
            }
        }
        catch ( TransactionNotPresentOnMasterException e )
        {
            // This is OK. This method has been called due to some connection problem or similar,
            // it's a best-effort to finish of a channel and transactions associated with it.
        }
    }

    public Map<Integer, Collection<RequestContext>> getSlaveInformation()
    {
        // Which slaves are connected a.t.m?
        Set<Integer> machineIds = new HashSet<>();
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
        return new TreeMap<>( ongoingTransactions );
    }
}
