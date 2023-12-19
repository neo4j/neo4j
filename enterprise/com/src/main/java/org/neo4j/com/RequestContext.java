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
package org.neo4j.com;


/**
 * A representation of the context in which an HA slave operates.
 * Contains
 * <ul>
 * <li>the machine id</li>
 * <li>a list of the last applied transaction id for each datasource</li>
 * <li>an event identifier, the txid of the most recent local top level tx</li>
 * <li>a session id, the startup time of the database</li>
 * </ul>
 */
public final class RequestContext
{
    private final int machineId;
    private final long lastAppliedTransaction;
    private final int eventIdentifier;
    private final int hashCode;
    private final long epoch;
    private final long checksum;

    public RequestContext( long epoch, int machineId, int eventIdentifier,
            long lastAppliedTransaction, long checksum )
    {
        this.epoch = epoch;
        this.machineId = machineId;
        this.eventIdentifier = eventIdentifier;
        this.lastAppliedTransaction = lastAppliedTransaction;
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

    public long lastAppliedTransaction()
    {
        return lastAppliedTransaction;
    }

    public int getEventIdentifier()
    {
        return eventIdentifier;
    }

    public long getEpoch()
    {
        return epoch;
    }

    public long getChecksum()
    {
        return checksum;
    }

    @Override
    public String toString()
    {
        return "RequestContext[" +
                "machineId=" + machineId +
                ", lastAppliedTransaction=" + lastAppliedTransaction +
                ", eventIdentifier=" + eventIdentifier +
                ", hashCode=" + hashCode +
                ", epoch=" + epoch +
                ", checksum=" + checksum +
                ']';
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

    public static final RequestContext EMPTY = new RequestContext( -1, -1, -1, -1, -1 );

    public static RequestContext anonymous( long lastAppliedTransaction )
    {
        return new RequestContext( EMPTY.epoch, EMPTY.machineId, EMPTY.eventIdentifier,
                lastAppliedTransaction, EMPTY.checksum );
    }
}
