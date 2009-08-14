/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction;

import java.nio.ByteBuffer;

import javax.transaction.xa.Xid;

public class XidImpl implements Xid
{
    private static final int FORMAT_ID = 0x4E454E31; // NEO format identidier

    // hardcoded for now, we don't have distribution yet but
    // this should change later
    private static final byte INSTANCE_ID[] = new byte[] { 'N', 'E', 'O', 'K',
        'E', 'R', 'N', 'L', '\0' };

    private static long nextSequenceId = 0;

    // next sequence number for global tx id
    private synchronized static long getNextSequenceId()
    {
        return nextSequenceId++;
    }

    // INSTANCE_ID + millitime(long) + seqnumber(long)
    private final byte globalId[];
    // branchId assumes Xid.MAXBQUALSIZE >= 4
    private final byte branchId[];
    private final int formatId;
    

    // resourceId.length = 4, unique for each XAResource
    static byte[] getNewGlobalId()
    {
        // create new global id ( [INSTANCE_ID][time][sequence] )
        byte globalId[] = new byte[INSTANCE_ID.length + 16];
        System.arraycopy( INSTANCE_ID, 0, globalId, 0, INSTANCE_ID.length );
        ByteBuffer byteBuf = ByteBuffer.wrap( globalId );
        byteBuf.position( INSTANCE_ID.length );
        long time = System.currentTimeMillis();
        long sequence = getNextSequenceId();
        byteBuf.putLong( time ).putLong( sequence );
        return globalId;
    }

    static boolean isThisTm( byte globalId[] )
    {
        if ( globalId.length < INSTANCE_ID.length )
        {
            return false;
        }
        for ( int i = 0; i < INSTANCE_ID.length; i++ )
        {
            if ( globalId[i] != INSTANCE_ID[i] )
            {
                return false;
            }
        }
        return true;
    }
    
    public XidImpl( byte globalId[], byte resourceId[] )
    {
        this( globalId, resourceId, FORMAT_ID );
    }

    // create xid for transaction with more than one XAResource enlisted
    public XidImpl( byte globalId[], byte resourceId[], int formatId )
    {
        if ( globalId.length > Xid.MAXGTRIDSIZE )
        {
            throw new IllegalArgumentException( "GlobalId length to long, "
                + globalId.length );
        }
        if ( resourceId.length > Xid.MAXBQUALSIZE )
        {
            throw new IllegalArgumentException(
                "BranchId (resource id) to long, " + resourceId.length );
        }
        this.globalId = globalId;
        this.branchId = resourceId;
        this.formatId = formatId;
    }

    public byte[] getGlobalTransactionId()
    {
        return globalId.clone();
    }

    public byte[] getBranchQualifier()
    {
        return branchId.clone();
    }

    public int getFormatId()
    {
        return FORMAT_ID;
    }

    public boolean equals( Object o )
    {
        if ( !(o instanceof Xid) )
        {
            return false;
        }
        byte otherGlobalId[] = ((Xid) o).getGlobalTransactionId();
        byte otherBranchId[] = ((Xid) o).getBranchQualifier();

        if ( globalId.length != otherGlobalId.length || 
            branchId.length != otherBranchId.length )
        {
            return false;
        }

        for ( int i = 0; i < globalId.length; i++ )
        {
            if ( globalId[i] != otherGlobalId[i] )
            {
                return false;
            }
        }
        for ( int i = 0; i < branchId.length; i++ )
        {
            if ( branchId[i] != otherBranchId[i] )
            {
                return false;
            }
        }
        return true;
    }

    private volatile int hashCode = 0;

    public int hashCode()
    {
        if ( hashCode == 0 )
        {
            int calcHash = 0;
            for ( int i = 0; i < 3 && i < globalId.length; i++ )
            {
                calcHash += globalId[globalId.length - i - 1] << i * 8;
            }
            if ( branchId.length > 0 )
            {
                calcHash += branchId[0] << 3 * 8;
            }
            hashCode = 3217 * calcHash;
        }
        return hashCode;
    }

    public String toString()
    {
        StringBuffer buf = new StringBuffer( "GlobalId[" );
        if ( globalId.length == (INSTANCE_ID.length + 8 + 8) )
        {
            for ( int i = 0; i < INSTANCE_ID.length - 1; i++ )
            {
                buf.append( (char) globalId[i] );
            }
            ByteBuffer byteBuf = ByteBuffer.wrap( globalId );
            byteBuf.position( INSTANCE_ID.length );
            long time = byteBuf.getLong();
            long sequence = byteBuf.getLong();
            buf.append( '|' );
            buf.append( time );
            buf.append( '|' );
            buf.append( sequence );
        }
        else
        {
            buf.append( "UNKNOWN_ID" );
        }
        buf.append( "], BranchId[ " );
        for ( int i = 0; i < branchId.length; i++ )
        {
            buf.append( branchId[i] + " " );
        }
        buf.append( "]" );
        return buf.toString();
    }
}