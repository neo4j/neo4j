/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.function.Supplier;

import org.neo4j.com.ComException;
import org.neo4j.com.ComExceptionHandler;
import org.neo4j.com.IllegalProtocolVersionException;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.MasterClient310;
import org.neo4j.kernel.ha.MasterClient320;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class MasterClientResolver implements MasterClientFactory, ComExceptionHandler
{
    private volatile MasterClientFactory currentFactory;

    private final Map<ProtocolVersion, MasterClientFactory> protocolToFactoryMapping;
    private final Log log;
    private final ResponseUnpacker responseUnpacker;
    private final InvalidEpochExceptionHandler invalidEpochHandler;
    private final Supplier<LogEntryReader<ReadableClosablePositionAwareChannel>> logEntryReader;

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
            int readTimeoutMillis, int lockReadTimeout, int channels, int chunkSize,
            Supplier<LogEntryReader<ReadableClosablePositionAwareChannel>> logEntryReader )
    {
        this.logEntryReader = logEntryReader;
        this.log = logProvider.getLog( getClass() );
        this.responseUnpacker = responseUnpacker;
        this.invalidEpochHandler = invalidEpochHandler;

        protocolToFactoryMapping = new HashMap<>( 3, 1 );
        protocolToFactoryMapping.put( MasterClient214.PROTOCOL_VERSION, new F214( logProvider, readTimeoutMillis, lockReadTimeout,
                channels, chunkSize ) );
        protocolToFactoryMapping.put( MasterClient310.PROTOCOL_VERSION, new F310( logProvider, readTimeoutMillis, lockReadTimeout,
                channels, chunkSize ) );
        protocolToFactoryMapping.put( MasterClient320.PROTOCOL_VERSION, new F320( logProvider, readTimeoutMillis, lockReadTimeout,
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
        return getFor( MasterClient320.PROTOCOL_VERSION );
    }

    private abstract static class StaticMasterClientFactory implements MasterClientFactory
    {
        final LogProvider logProvider;
        final int readTimeoutMillis;
        final int lockReadTimeout;
        final int maxConcurrentChannels;
        final int chunkSize;

        StaticMasterClientFactory( LogProvider logProvider, int readTimeoutMillis, int lockReadTimeout,
                                   int maxConcurrentChannels, int chunkSize )
        {
            this.logProvider = logProvider;
            this.readTimeoutMillis = readTimeoutMillis;
            this.lockReadTimeout = lockReadTimeout;
            this.maxConcurrentChannels = maxConcurrentChannels;
            this.chunkSize = chunkSize;
        }
    }

    private final class F214 extends StaticMasterClientFactory
    {
        private F214( LogProvider logProvider, int readTimeoutMillis, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logProvider, readTimeoutMillis, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                Monitors monitors, StoreId storeId, LifeSupport life )
        {
            return life.add( new MasterClient214(  destinationHostNameOrIp, destinationPort, originHostNameOrIp,
                    logProvider, storeId, readTimeoutMillis, lockReadTimeout, maxConcurrentChannels, chunkSize,
                    responseUnpacker, monitors.newMonitor( ByteCounterMonitor.class, MasterClient320.class ),
                    monitors.newMonitor( RequestMonitor.class, MasterClient320.class ), logEntryReader.get() ) );
        }
    }

    private final class F310 extends StaticMasterClientFactory
    {
        private F310( LogProvider logProvider, int readTimeoutMillis, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logProvider, readTimeoutMillis, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                Monitors monitors, StoreId storeId, LifeSupport life )
        {
            return life.add( new MasterClient310(  destinationHostNameOrIp, destinationPort, originHostNameOrIp,
                    logProvider, storeId, readTimeoutMillis, lockReadTimeout, maxConcurrentChannels, chunkSize,
                    responseUnpacker, monitors.newMonitor( ByteCounterMonitor.class, MasterClient320.class ),
                    monitors.newMonitor( RequestMonitor.class, MasterClient320.class ), logEntryReader.get() ) );
        }
    }

    private final class F320 extends StaticMasterClientFactory
    {
        private F320( LogProvider logProvider, int readTimeoutMillis, int lockReadTimeout, int maxConcurrentChannels,
                     int chunkSize )
        {
            super( logProvider, readTimeoutMillis, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String destinationHostNameOrIp, int destinationPort, String originHostNameOrIp,
                Monitors monitors, StoreId storeId, LifeSupport life )
        {
            return life.add( new MasterClient320(  destinationHostNameOrIp, destinationPort, originHostNameOrIp,
                    logProvider, storeId, readTimeoutMillis, lockReadTimeout, maxConcurrentChannels, chunkSize,
                    responseUnpacker, monitors.newMonitor( ByteCounterMonitor.class, MasterClient320.class ),
                    monitors.newMonitor( RequestMonitor.class, MasterClient320.class ), logEntryReader.get() ) );
        }
    }
}
