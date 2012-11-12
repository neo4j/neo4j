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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.com.MismatchingVersionHandler;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class MasterClientResolver implements MasterClientFactory, MismatchingVersionHandler
{
    private volatile MasterClientFactory currentFactory;
    private volatile ProtocolVersionCombo currentVersion;
    private boolean downgradeForbidden = false;

    @Override
    public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId, LifeSupport life )
    {
        MasterClient result = currentFactory.instantiate( hostNameOrIp, port, storeId, life );
        result.addMismatchingVersionHandler( this );
        return result;
    }

    @Override
    public void versionMismatched( int expected, int received )
    {
        getFor( received, 2 );
    }

    private static final class ProtocolVersionCombo implements Comparable<ProtocolVersionCombo>
    {
        final int applicationProtocol;
        final int internalProtocol;

        ProtocolVersionCombo( int applicationProtocol, int internalProtocol )
        {
            this.applicationProtocol = applicationProtocol;
            this.internalProtocol = internalProtocol;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj == null )
            {
                return false;
            }
            if ( obj.getClass() != ProtocolVersionCombo.class )
            {
                return false;
            }
            ProtocolVersionCombo other = (ProtocolVersionCombo) obj;
            return other.applicationProtocol == applicationProtocol && other.internalProtocol == internalProtocol;
        }

        @Override
        public int hashCode()
        {
            return ( 31 * applicationProtocol ) | internalProtocol;
        }

        @Override
        public int compareTo( ProtocolVersionCombo o )
        {
            return ( applicationProtocol < o.applicationProtocol ? -1
                    : ( applicationProtocol == o.applicationProtocol ? 0 : 1 ) );
        }

        static final ProtocolVersionCombo PC_153 = new ProtocolVersionCombo( 2, 2 );
        static final ProtocolVersionCombo PC_17 = new ProtocolVersionCombo( 3, 2 );
        static final ProtocolVersionCombo PC_18 = new ProtocolVersionCombo( 4, 2 );
    }

    private final Map<ProtocolVersionCombo, MasterClientFactory> protocolToFactoryMapping;

    public MasterClientResolver( StringLogger messageLogger, int readTimeout, int lockReadTimeout, int channels,
            int chunkSize )
    {
        protocolToFactoryMapping = new HashMap<ProtocolVersionCombo, MasterClientFactory>();
        protocolToFactoryMapping.put( ProtocolVersionCombo.PC_153, new F153( messageLogger, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
        protocolToFactoryMapping.put( ProtocolVersionCombo.PC_17, new F17( messageLogger, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
        protocolToFactoryMapping.put( ProtocolVersionCombo.PC_18, new F18( messageLogger, readTimeout, lockReadTimeout,
                channels, chunkSize ) );
    }

    public MasterClientFactory getFor( int applicationProtocol, int internalProtocol )
    {
        ProtocolVersionCombo incomingCombo = new ProtocolVersionCombo( applicationProtocol, internalProtocol );
        MasterClientFactory candidate = protocolToFactoryMapping.get( incomingCombo );
        /*
         * Things that can happen here regarding replacing the current factory, in order:
         * 1. We do not know the protocol - candidate is null: We don't change the current factory
         * 2. The current factory is null: We always set it to the latest requested
         * 3. We receive a version newer than the current one: Always replace the current factory
         * 4. We receive a version older than the current: Replace if downgrades are allowed, else leave as is.
         */
        if ( ( candidate != null )
             && ( currentVersion == null || !downgradeForbidden || currentVersion.compareTo( incomingCombo ) <= 0 ) )
        {
            currentFactory = candidate;
            currentVersion = incomingCombo;
        }
        return candidate;
    }

    public MasterClientFactory getDefault()
    {
        return getFor( ProtocolVersionCombo.PC_18.applicationProtocol, ProtocolVersionCombo.PC_18.internalProtocol );
    }

    protected static abstract class StaticMasterClientFactory implements MasterClientFactory
    {
        protected final StringLogger stringLogger;
        protected final int readTimeoutSeconds;
        protected final int lockReadTimeout;
        protected final int maxConcurrentChannels;
        protected final int chunkSize;

        StaticMasterClientFactory( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout,
                int maxConcurrentChannels, int chunkSize )
        {
            this.stringLogger = stringLogger;
            this.readTimeoutSeconds = readTimeoutSeconds;
            this.lockReadTimeout = lockReadTimeout;
            this.maxConcurrentChannels = maxConcurrentChannels;
            this.chunkSize = chunkSize;
        }
    }

    public static final class F153 extends StaticMasterClientFactory
    {
        public F153( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                int chunkSize )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId, LifeSupport life )
        {
            return life.add(  new MasterClient153( hostNameOrIp, port, stringLogger, storeId,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize ) );
        }
    };

    public static final class F17 extends StaticMasterClientFactory
    {
        public F17( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                int chunkSize )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId, LifeSupport life )
        {
            return life.add(  new MasterClient17( hostNameOrIp, port, stringLogger, storeId,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize ) );
        }
    };

    public static final class F18 extends StaticMasterClientFactory
    {
        public F18( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels,
                int chunkSize )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId, LifeSupport life )
        {
            return life.add( new MasterClient18( hostNameOrIp, port, stringLogger, storeId,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels, chunkSize ) );
        }
    }

    public void enableDowngradeBarrier()
    {
        downgradeForbidden = true;
    }
}
