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
package org.neo4j.kernel.ha.com.master;

import org.neo4j.com.Protocol;
import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;
import org.neo4j.com.Server;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.kernel.ha.HaRequestType210;
import org.neo4j.kernel.ha.MasterClient320;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Clocks;

import static org.neo4j.kernel.ha.com.slave.MasterClient.CURRENT;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link org.neo4j.kernel.ha.com.slave.MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer extends Server<Master, Void>
{
    public static final int FRAME_LENGTH = Protocol.DEFAULT_FRAME_LENGTH;
    private final ConversationManager conversationManager;
    private final HaRequestType210 requestTypes;

    public MasterServer( Master requestTarget, LogProvider logProvider, Configuration config,
                         TxChecksumVerifier txVerifier, ByteCounterMonitor byteCounterMonitor,
                         RequestMonitor requestMonitor, ConversationManager conversationManager,
                         LogEntryReader<ReadableClosablePositionAwareChannel> entryReader )
    {
        super( requestTarget, config, logProvider, FRAME_LENGTH, CURRENT, txVerifier,
                Clocks.systemClock(), byteCounterMonitor, requestMonitor );
        this.conversationManager = conversationManager;
        this.requestTypes = new HaRequestType210( entryReader, MasterClient320.LOCK_RESULT_OBJECT_SERIALIZER );
    }

    @Override
    protected RequestType getRequestContext( byte id )
    {
        return requestTypes.type( id );
    }

    @Override
    protected void stopConversation( RequestContext context )
    {
        conversationManager.stop( context );
    }
}
