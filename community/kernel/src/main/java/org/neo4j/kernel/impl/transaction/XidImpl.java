/**
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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import javax.transaction.xa.Xid;

import static java.nio.ByteBuffer.wrap;

public class XidImpl implements Xid
{
    interface Seed
    {
        long nextRandomLong();
        
        long nextSequenceId();
    }
    
    public static final Seed DEFAULT_SEED = new Seed()
    {
        private long nextSequenceId = 0;
        private final Random r = new Random();

        @Override
        public synchronized long nextSequenceId()
        {
            return nextSequenceId++;
        }
        
        @Override
        public long nextRandomLong()
        {
            return r.nextLong();
        }
    };
    
    // Neo4j ('N' 'E' 'O') format identifier
    private static final int FORMAT_ID = 0x4E454E31;

    private static final byte INSTANCE_ID[] = new byte[] { 'N', 'E', 'O', 'K',
        'E', 'R', 'N', 'L', '\0' };

    // INSTANCE_ID + millitime(long) + seqnumber(long)
    private final byte globalId[];
    // branchId assumes Xid.MAXBQUALSIZE >= 4
    private final byte branchId[];

    /**
     * Generates a new global id byte[] for use as one part that makes up a Xid. A global id is made up of:
     * - INSTANCE_ID: fixed number of bytes and content
     * - 8b: current time millis
     * - 8b: sequence id, incremented for each new generated global id
     * - 4b: local id, an id fixed for and local to the instance generating this global id
     * 
     * resourceId.length = 4, unique for each XAResource
     * @param localId
     * @return
     */
    public static byte[] getNewGlobalId( Seed seed, int localId )
    {
        byte globalId[] = Arrays.copyOf( INSTANCE_ID, INSTANCE_ID.length + 20 );
        wrap( globalId, INSTANCE_ID.length, 20 )
              .putLong( seed.nextRandomLong() )
              .putLong( seed.nextSequenceId() )
              .putInt( localId );
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
        if ( globalId.length > Xid.MAXGTRIDSIZE )
        {
            throw new IllegalArgumentException(
                    "GlobalId length to long: " + globalId.length + ". Max is " + Xid.MAXGTRIDSIZE );
        }
        if ( resourceId.length > Xid.MAXBQUALSIZE )
        {
            throw new IllegalArgumentException(
                "BranchId (resource id) to long, " + resourceId.length );
        }
        this.globalId = globalId;
        this.branchId = resourceId;
    }

    @Override
    public byte[] getGlobalTransactionId()
    {
        return globalId;
    }

    @Override
    public byte[] getBranchQualifier()
    {
        return branchId;
    }

    @Override
    public int getFormatId()
    {
        return FORMAT_ID;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !(o instanceof XidImpl) )
        {
            return false;
        }

        return Arrays.equals( globalId, ((XidImpl) o).globalId ) &&
               Arrays.equals( branchId, ((XidImpl) o).branchId );
    }

    private volatile int hashCode = 0;

    @Override
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

    @Override
    public String toString()
    {
        StringBuffer buf = new StringBuffer( "GlobalId[" );
        int baseLength = INSTANCE_ID.length + 8 + 8;
        if ( globalId.length == baseLength || globalId.length == baseLength+4 )
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
            
            /* MP 2013-11-27: 4b added to the globalId, consisting of the serverId value. This if-statement
             * keeps things backwards compatible and nice. */
            if ( byteBuf.hasRemaining() )
            {
                buf.append( '|' ).append( byteBuf.getInt() );
            }
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