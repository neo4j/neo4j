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
package org.neo4j.kernel.ha.com.slave;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.com.ComException;
import org.neo4j.com.ComExceptionHandler;
import org.neo4j.com.IllegalProtocolVersionException;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.kernel.ha.MasterClient201;
import org.neo4j.kernel.ha.MasterClient210;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

public class MasterClientResolver implements MasterClientFactory, ComExceptionHandler
{
    private volatile MasterClientFactory currentFactory;

    private final Map<ProtocolVersion,MasterClientFactory> protocolToFactoryMapping;
    private final StringLogger log;

    private final InvalidEpochExceptionHandler invalidEpochHandler;

    @Override
    public MasterClient instantiate( String hostNameOrIp, int port, Monitors monitors, StoreId storeId )
    {
        if ( currentFactory == null )
        {
            assignDefaultFactory();
        }

        MasterClient result = currentFactory.instantiate( hostNameOrIp, port, monitors, storeId );
        result.setComExceptionHandler( this );
        return result;
    }

    public MasterClientResolver( Logging logging, InvalidEpochExceptionHandler invalidEpochHandler,
            int readTimeout, int lockReadTimeout, int channels, int chunkSize )
    {
        this.log = logging.getMessagesLog( getClass() );
        this.invalidEpochHandler = invalidEpochHandler;

        protocolToFactoryMapping = new HashMap<>( 3, 1 );
        protocolToFactoryMapping.put( MasterClient201.PROTOCOL_VERSION, new F201( logging, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
        protocolToFactoryMapping.put( MasterClient210.PROTOCOL_VERSION, new F210( logging, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
        protocolToFactoryMapping.put( MasterClient214.PROTOCOL_VERSION, new F214( logging, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
    }

    @Override
    public void handle( ComException exception )
    {
        if ( exception instanceof IllegalProtocolVersionException )
        {
            log.info( "Handling " + exception + ", will pick new master client" );

            IllegalProtocolVersionException illegalProtocolVersion = (IllegalProtocolVersionException) exception;
            ProtocolVersion requiredProtocolVersion = new ProtocolVersion( illegalProtocolVersion.getReceived(),
                    ProtocolVersion.INTERNAL_PROTOCOL_VERSION );
            getFor( requiredProtocolVersion );
        }
        else if ( exception instanceof InvalidEpochException )
        {
            log.info( "Handling " + exception + ", will go to PENDING and ask for election" );

            invalidEpochHandler.handle();
        }
    }

    private MasterClientFactory getFor( ProtocolVersion protocolVersion )
    {
        MasterClientFactory candidate = protocolToFactoryMapping.get( protocolVersion );
        if ( candidate != null )
        {
            currentFactory = candidate;
        }
        return candidate;
    }

    private MasterClientFactory assignDefaultFactory()
    {
        return getFor( MasterClient214.PROTOCOL_VERSION );
    }

    private abstract static class StaticMasterClientFactory implements MasterClientFactory
    {
        protected final Logging logging;
        protected final int readTimeoutSeconds;
        protected final int lockReadTimeout;
        protected final int maxConcurrentChannels;
        protected final int chunkSize;

        StaticMasterClientFactory( Logging logging, int readTimeoutSeconds, int lockReadTimeout,
                                   int maxConcurrentChannels, int chunkSize )
        {
            this.logging = logging;
            this.readTimeoutSeconds = readTimeoutSeconds;
            this.lockReadTimeout = lockReadTimeout;
            this.maxConcurrentChannels = maxConcurrentChannels;
            this.chunkSize = chunkSize;
        }
    }

    private static final class F201 extends StaticMasterClientFactory
    {
        public F201( Logging logging, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logging, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, Monitors monitors, StoreId storeId )
        {
            return new MasterClient201( hostNameOrIp, port, logging, monitors, storeId,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }
    }

    private static final class F210 extends StaticMasterClientFactory
    {
        public F210( Logging logging, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logging, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, Monitors monitors, StoreId storeId )
        {
            return new MasterClient210( hostNameOrIp, port, logging, monitors, storeId,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }
    }

    private static final class F214 extends StaticMasterClientFactory
    {
        public F214( Logging logging, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logging, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, Monitors monitors, StoreId storeId )
        {
            return new MasterClient214( hostNameOrIp, port, logging, monitors, storeId,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }
    }
}
