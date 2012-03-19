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
import static org.junit.Assert.assertTrue;

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
        assertCorrectChecksumEquality( randomXid( Boolean.TRUE ), true );

        // Negative Xid hashcode
        assertCorrectChecksumEquality( randomXid( Boolean.FALSE ), false );
    }

    private void assertCorrectChecksumEquality( Xid refXid, boolean positive )
    {
        Start ref = new Start( refXid, refId, refMaster, refMe, startPosition, refTime );
        assertChecksumsEquals( ref, new Start( refXid, refId, refMaster, refMe, startPosition, refTime ) );

        // Different Xids
        assertChecksumsNotEqual( ref, new Start( randomXid( null ), refId, refMaster, refMe, startPosition, refTime ),
                true, positive );

        // Different master
        assertChecksumsNotEqual( ref, new Start( refXid, refId, refMaster + 1, refMe, startPosition, refTime ), false,
                positive );

        // Different me
        assertChecksumsNotEqual( ref, new Start( refXid, refId, refMaster, refMe + 1, startPosition, refTime ), false,
                positive );
    }

    private void assertChecksumsNotEqual( Start ref, Start other, boolean differentXid, boolean positive )
    {
        if ( positive || differentXid )
        {
            assertFalse( Start.checksumMatch( ref.getChecksum(), other.getChecksum() ) );
        }
        else
        {
            /*
             * same negative xids, means the checksums are equal no matter what.
             * Check all 32 high bits are set (this is the bug tested here)
             * Check the checksums are equal as numbers.
             * Check the LogEntry.Start method agrees
             */
            assertEquals( "not all high bits were set", 0xFFFFFFFF00000000L, ref.getChecksum() & 0xFFFFFFFF00000000L );
            assertEquals( "not all high bits were set", 0xFFFFFFFF00000000L, other.getChecksum() & 0xFFFFFFFF00000000L );
            assertEquals( ref.getChecksum(), other.getChecksum() );
            assertTrue( Start.checksumMatch( ref.getChecksum(), other.getChecksum() ) );
        }
    }

    private void assertChecksumsEquals( Start ref, Start other )
    {
        assertTrue( Start.checksumMatch( ref.getChecksum(), other.getChecksum() ) );
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
