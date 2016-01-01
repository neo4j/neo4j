/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.com.Protocol;
import org.neo4j.com.Protocol214;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.com.ProtocolVersion.INTERNAL_PROTOCOL_VERSION;

public class MasterClient214 extends MasterClient210
{
    public static final ProtocolVersion PROTOCOL_VERSION = new ProtocolVersion( (byte) 8, INTERNAL_PROTOCOL_VERSION );

    public MasterClient214( String hostNameOrIp, int port, Logging logging, Monitors monitors, StoreId storeId,
                            long readTimeoutSeconds, long lockReadTimeout, int maxConcurrentChannels, int chunkSize )
    {
        super( hostNameOrIp, port, logging, monitors, storeId, readTimeoutSeconds, lockReadTimeout,
                maxConcurrentChannels, chunkSize, PROTOCOL_VERSION );
    }

    @Override
    protected Protocol createProtocol( int chunkSize, byte applicationProtocolVersion )
    {
        return new Protocol214( chunkSize, applicationProtocolVersion, getInternalProtocolVersion() );
    }

    @Override
    public ProtocolVersion getProtocolVersion()
    {
        return PROTOCOL_VERSION;
    }
}
