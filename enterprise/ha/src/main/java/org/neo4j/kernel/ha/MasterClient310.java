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

import org.neo4j.com.Deserializer;
import org.neo4j.com.Protocol;
import org.neo4j.com.Protocol310;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;

import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;

public class MasterClient310 extends MasterClient214
{
    public static final ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion( (byte) 9, INTERNAL_PROTOCOL_VERSION );

    public MasterClient310( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                            LogProvider logProvider, StoreId storeId,
                            long readTimeoutMillis, long lockReadTimeout, int maxConcurrentChannels, int chunkSize,
                            ResponseUnpacker unpacker,
                            ByteCounterMonitor byteCounterMonitor,
                            RequestMonitor requestMonitor,
                            LogEntryReader<ReadableClosablePositionAwareChannel> entryReader )
    {
        super( destinationHostNameOrIp, destinationPort, originHostNameOrIp, logProvider, storeId, readTimeoutMillis,
                lockReadTimeout, maxConcurrentChannels, chunkSize, unpacker, byteCounterMonitor, requestMonitor,
                entryReader );
    }

    @Override
    protected Protocol createProtocol( int chunkSize, byte applicationProtocolVersion )
    {
        return new Protocol310( chunkSize, applicationProtocolVersion, getInternalProtocolVersion() );
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return PROTOCOL_VERSION;
    }

    @Override
    protected Deserializer<Void> createFileStreamDeserializer( StoreWriter writer )
    {
        return new Protocol.FileStreamsDeserializer310( writer );
    }
}
