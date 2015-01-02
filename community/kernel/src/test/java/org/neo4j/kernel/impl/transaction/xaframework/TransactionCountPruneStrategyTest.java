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
package org.neo4j.kernel.impl.transaction.xaframework;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TransactionCountPruneStrategyTest
{
    @Test
    public void shouldConsiderOldVersionLogsOutOfDateForTx() throws Exception
    {
        // Given
        LogPruneStrategies.TransactionCountPruneStrategy pruner = new LogPruneStrategies.TransactionCountPruneStrategy(null, 100);
        LogPruneStrategies.Threshold threshold = pruner.newThreshold();

        LogExtractor.LogLoader logLoader = mock( LogExtractor.LogLoader.class );
        when(logLoader.getFirstCommittedTxId( anyLong() )).thenThrow( new RuntimeException(new IllegalLogFormatException( 3, 2 )) );

        // When
        boolean reached = threshold.reached( null, 1, logLoader );

        // Then
        assertTrue(reached);
    }

    @Test
    public void shouldPanicIfHitNewerVersionLog() throws Exception
    {
        // Given
        // Given
        LogPruneStrategies.TransactionCountPruneStrategy pruner = new LogPruneStrategies.TransactionCountPruneStrategy(null, 100);
        LogPruneStrategies.Threshold threshold = pruner.newThreshold();

        LogExtractor.LogLoader logLoader = mock( LogExtractor.LogLoader.class );
        when(logLoader.getFirstCommittedTxId( anyLong() )).thenThrow( new RuntimeException(new IllegalLogFormatException( 3, 5 )) );


        try {

            // When
            threshold.reached( null, 1, logLoader );
            fail("Should've thrown exception");

        } catch(RuntimeException e) {

            // Then
            assertThat(e.getMessage(), equalTo("Unable to read database logs, because it contains logs from a newer " +
                    "version of Neo4j."));
        }
    }
}
