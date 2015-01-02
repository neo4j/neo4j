/**
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
package org.neo4j.com;

import java.util.Arrays;

/**
 * A representation of the context in which an HA slave operates. Contains <li>
 * the machine id</li> <li>a list of the last applied transaction id for each
 * datasource</li> <li>an event identifier, the txid of the most recent local
 * top level tx</li> <li>a session id, the startup time of the database</li>
 */
public final class RequestContext
{
    public static class Tx
    {
        private final String dataSourceName;
        private final long txId;

        public Tx( String dataSourceName, long txId )
        {
            this.dataSourceName = dataSourceName;
            this.txId = txId;
        }

        public String getDataSourceName()
        {
            return dataSourceName;
        }

        public long getTxId()
        {
            return txId;
        }

        @Override
        public String toString()
        {
            return dataSourceName + "/" + txId;
        }

    }

    public static Tx lastAppliedTx( String dataSourceName, long txId )
    {
        return new Tx( dataSourceName, txId );
    }

    private final int machineId;
    private final Tx[] lastAppliedTransactions;
    private final int eventIdentifier;
    private final int hashCode;
    private final long epoch;
    private final int masterId;
    private final long checksum;

    public RequestContext( long epoch, int machineId, int eventIdentifier,
            Tx[] lastAppliedTransactions, int masterId, long checksum )
    {
        this.epoch = epoch;
        this.machineId = machineId;
        this.eventIdentifier = eventIdentifier;
        this.lastAppliedTransactions = lastAppliedTransactions;
        this.masterId = masterId;
        this.checksum = checksum;

        long hash = epoch;
        hash = ( 31 * hash ) ^ eventIdentifier;
        hash = ( 31 * hash ) ^ machineId;
        this.hashCode = (int) ( ( hash >>> 32 ) ^ hash );
    }

    public int machineId()
    {
        return machineId;
    }

    public Tx[] lastAppliedTransactions()
    {
        return lastAppliedTransactions;
    }

    public int getEventIdentifier()
    {
        return eventIdentifier;
    }

    public long getEpoch()
    {
        return epoch;
    }

    public int getMasterId()
    {
        return masterId;
    }

    public long getChecksum()
    {
        return checksum;
    }

    @Override
    public String toString()
    {
        return "RequestContext[session: " + epoch + ", ID:" + machineId + ", eventIdentifier:" + eventIdentifier
               + ", " + Arrays.asList( lastAppliedTransactions ) + "]";
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof RequestContext ) )
        {
            return false;
        }
        RequestContext o = (RequestContext) obj;
        return o.eventIdentifier == eventIdentifier && o.machineId == machineId && o.epoch == epoch;
    }

    @Override
    public int hashCode()
    {
        return this.hashCode;
    }

    public static final RequestContext EMPTY = new RequestContext( -1, -1, -1, new Tx[0], -1, -1 );

    public static RequestContext anonymous( Tx[] lastAppliedTransactions )
    {
        return new RequestContext( EMPTY.epoch, EMPTY.machineId, EMPTY.eventIdentifier,
                lastAppliedTransactions, EMPTY.masterId, EMPTY.checksum );
    }
}
