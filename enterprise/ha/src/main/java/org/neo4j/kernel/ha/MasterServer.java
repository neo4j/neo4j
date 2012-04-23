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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jboss.netty.channel.Channel;
import org.neo4j.com.Protocol;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.kernel.ha.MasterClient.HaRequestType;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer extends Server<Master, Void>
{
    static final byte PROTOCOL_VERSION = 2;

    static final int FRAME_LENGTH = Protocol.DEFAULT_FRAME_LENGTH;
    private final TxChecksumVerifier txVerifier;

    public MasterServer( Master realMaster, final int port, StringLogger logger, int maxConcurrentTransactions,
            int oldChannelThreshold, TxChecksumVerifier txVerifier )
    {
        super( realMaster, port, logger, FRAME_LENGTH, PROTOCOL_VERSION, maxConcurrentTransactions,
                oldChannelThreshold, txVerifier );
        this.txVerifier = txVerifier;
    }

    @Override
    protected RequestType<Master> getRequestContext( byte id )
    {
        return HaRequestType.values()[id];
    }

    @Override
    protected void finishOffChannel( Channel channel, SlaveContext context )
    {
        getMaster().finishTransaction( context, false );
    }

    @Override
    public void shutdown()
    {
        getMaster().shutdown();
        super.shutdown();
    }
    
    @Override
    protected boolean shouldLogFailureToFinishOffChannel( Throwable failure )
    {
        return !( failure instanceof UnableToResumeTransactionException );
    }

    public Map<Integer, Collection<SlaveContext>> getSlaveInformation()
    {
        // Which slaves are connected a.t.m?
        Set<Integer> machineIds = new HashSet<Integer>();
        Map<Channel, SlaveContext> channels = getConnectedSlaveChannels();
        synchronized ( channels )
        {
            for ( SlaveContext context : channels.values() )
            {
                machineIds.add( context.machineId() );
            }
        }

        // Insert missing slaves into the map so that all connected slave
        // are in the returned map
        Map<Integer, Collection<SlaveContext>> ongoingTransactions =
                ((MasterImpl) getMaster()).getOngoingTransactions();
        for ( Integer machineId : machineIds )
        {
            if ( !ongoingTransactions.containsKey( machineId ) )
            {
                ongoingTransactions.put( machineId, Collections.<SlaveContext>emptyList() );
            }
        }
        return new TreeMap<Integer, Collection<SlaveContext>>( ongoingTransactions );
    }
}
