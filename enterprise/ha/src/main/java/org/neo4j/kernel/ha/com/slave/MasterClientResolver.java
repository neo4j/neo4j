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
package org.neo4j.kernel.ha.com.slave;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.com.ComException;
import org.neo4j.com.ComExceptionHandler;
import org.neo4j.com.IllegalProtocolVersionException;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.kernel.ha.MasterClient210;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;

public class MasterClientResolver implements MasterClientFactory, ComExceptionHandler
{
    private volatile MasterClientFactory currentFactory;

    private final Map<ProtocolVersion, MasterClientFactory> protocolToFactoryMapping;
    private final Log log;

    private final ResponseUnpacker responseUnpacker;
    private final InvalidEpochExceptionHandler invalidEpochHandler;

    @Override
    public MasterClient instantiate( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
            Monitors monitors, StoreId storeId, LifeSupport life )
    {
        if ( currentFactory == null )
        {
            assignDefaultFactory();
        }

        MasterClient result = currentFactory.instantiate( destinationHostNameOrIp, destinationPort, originHostNameOrIp,
                monitors, storeId, life );
        result.setComExceptionHandler( this );
        return result;
    }

    public MasterClientResolver( LogProvider logProvider, ResponseUnpacker responseUnpacker,
            InvalidEpochExceptionHandler invalidEpochHandler,
            int readTimeout, int lockReadTimeout, int channels, int chunkSize )
    {
        this.log = logProvider.getLog( getClass() );
        this.responseUnpacker = responseUnpacker;
        this.invalidEpochHandler = invalidEpochHandler;

        protocolToFactoryMapping = new HashMap<>( 2, 1 );
        protocolToFactoryMapping.put( MasterClient210.PROTOCOL_VERSION, new F210( logProvider, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
        protocolToFactoryMapping.put( MasterClient214.PROTOCOL_VERSION, new F214( logProvider, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
    }

    @Override
    public void handle( ComException exception )
    {
        exception.traceComException( log, "MasterClientResolver.handle" );
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
        else
        {
            log.debug( "Ignoring " + exception + "." );
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
        protected final LogProvider logProvider;
        protected final int readTimeoutSeconds;
        protected final int lockReadTimeout;
        protected final int maxConcurrentChannels;
        protected final int chunkSize;

        StaticMasterClientFactory( LogProvider logProvider, int readTimeoutSeconds, int lockReadTimeout,
                                   int maxConcurrentChannels, int chunkSize )
        {
            this.logProvider = logProvider;
            this.readTimeoutSeconds = readTimeoutSeconds;
            this.lockReadTimeout = lockReadTimeout;
            this.maxConcurrentChannels = maxConcurrentChannels;
            this.chunkSize = chunkSize;
        }
    }

    private final class F210 extends StaticMasterClientFactory
    {
        public F210( LogProvider logProvider, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logProvider, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                Monitors monitors, StoreId storeId, LifeSupport life )
        {
            return life.add(
                    new MasterClient210( destinationHostNameOrIp, destinationPort, originHostNameOrIp, logProvider,
                            storeId, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize,
                            responseUnpacker, monitors.newMonitor( ByteCounterMonitor.class, MasterClient210.class ),
                            monitors.newMonitor( RequestMonitor.class, MasterClient210.class ) ) );
        }
    }

    private final class F214 extends StaticMasterClientFactory
    {
        public F214( LogProvider logProvider, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logProvider, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                Monitors monitors, StoreId storeId, LifeSupport life )
        {
            return life.add(
                    new MasterClient214( destinationHostNameOrIp, destinationPort, originHostNameOrIp, logProvider,
                            storeId, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize,
                            responseUnpacker, monitors.newMonitor( ByteCounterMonitor.class, MasterClient214.class ),
                    monitors.newMonitor( RequestMonitor.class, MasterClient214.class ) ) );
        }
    }
}
