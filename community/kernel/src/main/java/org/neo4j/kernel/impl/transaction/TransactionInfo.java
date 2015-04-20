/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction;

public class TransactionInfo implements Comparable<TransactionInfo>
{
    private final int identifier;
    private final boolean trueForOnePhase;
    private final long txId;
    private final int master;
    private final long checksum;
    
    public TransactionInfo( int identifier, boolean trueForOnePhase, long txId, int master, long checksum )
    {
        super();
        this.identifier = identifier;
        this.trueForOnePhase = trueForOnePhase;
        this.txId = txId;
        this.master = master;
        this.checksum = checksum;
    }
    
    public int getIdentifier()
    {
        return identifier;
    }
    
    public boolean isOnePhase()
    {
        return trueForOnePhase;
    }
    
    public long getTxId()
    {
        return txId;
    }
    
    public int getMaster()
    {
        return master;
    }
    
    public long getChecksum()
    {
        return checksum;
    }

    @Override
    public int hashCode()
    {
        return identifier;
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj instanceof TransactionInfo && ((TransactionInfo)obj).identifier == identifier;
    }

    @Override
    public int compareTo( TransactionInfo o )
    {
        return Long.valueOf( txId ).compareTo( Long.valueOf( o.txId ) );
    }
}
