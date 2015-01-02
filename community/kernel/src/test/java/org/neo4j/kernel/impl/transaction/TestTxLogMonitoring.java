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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TargetDirectory;

public class TestTxLogMonitoring
{
    private static final TargetDirectory target = TargetDirectory.forTest( TestTxLogMonitoring.class );

    @Test
    public void shouldCountBytesWritten() throws Exception
    {
        // Given
        File directory = target.cleanDirectory( "shouldCountBytesWritten" );
        File theLogFile = new File( directory, "theLog" );
        Monitors monitors = new Monitors();

        TxLog txLog = new TxLog( theLogFile, new DefaultFileSystemAbstraction(), monitors );
        final AtomicLong bytesWritten = new AtomicLong();
        monitors.addMonitorListener( new ByteCounterMonitor()
        {
            @Override
            public void bytesWritten( long numberOfBytes )
            {
                bytesWritten.addAndGet( numberOfBytes );
            }

            @Override
            public void bytesRead( long numberOfBytes )
            {
            }
        }, TxLog.class.getName() );

        byte[] globalId = {1, 2, 3};

        // When
        txLog.txStart( globalId );
        txLog.addBranch( globalId, new byte[]{4,5,6} );
        txLog.close();

        // Then
        assertTrue( bytesWritten.get() > 0 );
        assertEquals( theLogFile.length(), bytesWritten.get() );
    }
}
