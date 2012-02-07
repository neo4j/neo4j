/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Random;

import javax.transaction.xa.Xid;

import org.junit.Test;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;

public class TestTxEntries
{
    private final Random random = new Random();
    private final long refTime = System.currentTimeMillis();
    private final int refId = 1;
    private final int refMaster = 1;
    private final int refMe = 1;
    private final long startPosition = 1000;

    @Test
    public void startEntryShouldBeUniqueIfEitherValueChanges() throws Exception
    {
        // Positive Xid hashcode
        assertorrectChecksumEquality( randomXid( Boolean.TRUE ) );
        
        // Negative Xid hashcode
        assertorrectChecksumEquality( randomXid( Boolean.FALSE ) );
    }

    private void assertorrectChecksumEquality( Xid refXid )
    {
        Start ref = new Start( refXid, refId, refMaster, refMe, startPosition, refTime ); 
        assertChecksumsEquals( ref, new Start( refXid, refId, refMaster, refMe, startPosition, refTime ) );
        
        // Different Xids
        assertChecksumsNotEqual( ref, new Start( randomXid( null ), refId, refMaster, refMe, startPosition, refTime ) );

        // Different master
        assertChecksumsNotEqual( ref, new Start( refXid, refId, refMaster+1, refMe, startPosition, refTime ) );

        // Different me
        assertChecksumsNotEqual( ref, new Start( refXid, refId, refMaster, refMe+1, startPosition, refTime ) );
    }

    private void assertChecksumsNotEqual( Start ref, Start other )
    {
        assertFalse( ref.getChecksum() == other.getChecksum() );
    }

    private void assertChecksumsEquals( Start ref, Start other )
    {
        assertEquals( ref.getChecksum(), other.getChecksum() );
    }

    private Xid randomXid( Boolean trueForPositive )
    {
        while ( true )
        {
            Xid xid = new XidImpl( randomBytes(), randomBytes() );
            if ( trueForPositive == null || xid.hashCode() > 0 == trueForPositive.booleanValue() ) return xid;
        }
    }

    private byte[] randomBytes()
    {
        byte[] bytes = new byte[random.nextInt( 10 )+5];
        for ( int i = 0; i < bytes.length; i++ ) bytes[i] = (byte) random.nextInt( 255 );
        return bytes;
    }
}
