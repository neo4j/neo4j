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

import org.neo4j.com.ConnectionLostHandler;
import org.neo4j.com.MismatchingVersionHandler;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

public class MasterClientResolver implements MasterClientFactory, MismatchingVersionHandler
{
    private volatile MasterClientFactory current;

    @Override
    public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId )
    {
        MasterClient result = current.instantiate( hostNameOrIp, port, storeId );
        result.addMismatchingVersionHandler( this );
        return result;
    }

    @Override
    public void versionMismatched( int expected, int received )
    {
        getFor( received, 2 );
    }

    private static final class ProtocolCombo
    {
        final int applicationProtocol;
        final int internalProtocol;

        ProtocolCombo( int applicationProtocol, int internalProtocol )
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
            if ( obj.getClass() != ProtocolCombo.class )
            {
                return false;
            }
            ProtocolCombo other = (ProtocolCombo) obj;
            return other.applicationProtocol == applicationProtocol && other.internalProtocol == internalProtocol;
        }

        @Override
        public int hashCode()
        {
            return ( 31 * applicationProtocol ) | internalProtocol;
        }

        static final ProtocolCombo PC_153 = new ProtocolCombo( 2, 2 );
        static final ProtocolCombo PC_17 = new ProtocolCombo( 3, 2 );
        static final ProtocolCombo PC_18 = new ProtocolCombo( 4, 2 );
    }

    private final Map<ProtocolCombo, MasterClientFactory> protocolToFactoryMapping;

    public MasterClientResolver( StringLogger messageLogger, int readTimeout, int lockReadTimeout, int channels )
    {
        protocolToFactoryMapping = new HashMap<ProtocolCombo, MasterClientFactory>();
        protocolToFactoryMapping.put( ProtocolCombo.PC_153, new F153( messageLogger, readTimeout, lockReadTimeout,
                channels ) );
        protocolToFactoryMapping.put( ProtocolCombo.PC_17, new F17( messageLogger, readTimeout, lockReadTimeout,
                channels ) );
        protocolToFactoryMapping.put( ProtocolCombo.PC_18, new F18( messageLogger, readTimeout, lockReadTimeout,
                channels ) );
    }

    public MasterClientFactory getFor( int applicationProtocol, int internalProtocol )
    {
        MasterClientFactory candidate = protocolToFactoryMapping.get( new ProtocolCombo( applicationProtocol,
                internalProtocol ) );
        /*
         * There is the possibility that we got a protocol we do not recognize, such as a slave talking to a SlaveServer.
         * In this case the proper action is no action, just keep the old version around.
         */
        if ( candidate != null ) current = candidate;
        return candidate;
    }

    public MasterClientFactory getDefault()
    {
        return getFor( ProtocolCombo.PC_18.applicationProtocol, ProtocolCombo.PC_18.internalProtocol );
    }

    protected static abstract class StaticMasterClientFactory implements MasterClientFactory
    {
        protected final StringLogger stringLogger;
        protected final int readTimeoutSeconds;
        protected final int lockReadTimeout;
        protected final int maxConcurrentChannels;

        StaticMasterClientFactory( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout,
                int maxConcurrentChannels )
        {
            this.stringLogger = stringLogger;
            this.readTimeoutSeconds = readTimeoutSeconds;
            this.lockReadTimeout = lockReadTimeout;
            this.maxConcurrentChannels = maxConcurrentChannels;
        }
    }

    public static final class F153 extends StaticMasterClientFactory
    {
        public F153( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId )
        {
            return new MasterClient153( hostNameOrIp, port, stringLogger, storeId, ConnectionLostHandler.NO_ACTION,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }
    };

    public static final class F17 extends StaticMasterClientFactory
    {
        public F17( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId )
        {
            return new MasterClient17( hostNameOrIp, port, stringLogger, storeId, ConnectionLostHandler.NO_ACTION,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }
    };

    public static final class F18 extends StaticMasterClientFactory
    {
        public F18( StringLogger stringLogger, int readTimeoutSeconds, int lockReadTimeout, int maxConcurrentChannels )
        {
            super( stringLogger, readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }

        @Override
        public MasterClient instantiate( String hostNameOrIp, int port, StoreId storeId )
        {
            return new MasterClient18( hostNameOrIp, port, stringLogger, storeId, ConnectionLostHandler.NO_ACTION,
                    readTimeoutSeconds, lockReadTimeout, maxConcurrentChannels );
        }
    };
}
