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

import org.junit.Test;

import org.neo4j.logging.Log;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LoggingResourcePoolMonitorTest
{
    @Test
    public void testUpdatedCurrentPeakSizeLogsOnlyOnChange()
    {
        Log log = mock( Log.class );
        LoggingResourcePoolMonitor monitor = new LoggingResourcePoolMonitor( log );

        monitor.updatedCurrentPeakSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedCurrentPeakSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedCurrentPeakSize( 11 );
        verify( log, times( 2 ) ).debug( anyString() );
    }

    @Test
    public void testUpdatedTargetSizeOnlyOnChange()
    {
        Log log = mock( Log.class );
        LoggingResourcePoolMonitor monitor = new LoggingResourcePoolMonitor( log );

        monitor.updatedTargetSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedTargetSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedTargetSize( 11 );
        verify( log, times( 2 ) ).debug( anyString() );
    }
}
