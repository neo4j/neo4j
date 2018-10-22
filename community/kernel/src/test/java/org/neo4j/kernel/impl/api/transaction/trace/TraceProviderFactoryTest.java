/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.transaction.trace;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.api.transaction.trace.TraceProviderFactory.getTraceProvider;
import static org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace.NONE;
import static org.neo4j.kernel.impl.api.transaction.trace.TransactionTracingLevel.ALL;
import static org.neo4j.kernel.impl.api.transaction.trace.TransactionTracingLevel.DISABLED;
import static org.neo4j.kernel.impl.api.transaction.trace.TransactionTracingLevel.SAMPLE;

class TraceProviderFactoryTest
{

    @Test
    void disabledTracerCreation()
    {
        Config config = Config.defaults( GraphDatabaseSettings.transaction_tracing_level, DISABLED.name() );
        TraceProvider traceProvider = getTraceProvider( config );
        for ( int i = 0; i < 100; i++ )
        {
            assertSame( NONE, traceProvider.getTraceInfo() );
        }
    }

    @Test
    void samplingTracerCreation()
    {
        Config config = Config.defaults( GraphDatabaseSettings.transaction_tracing_level, SAMPLE.name() );
        config.augment( GraphDatabaseSettings.transaction_sampling_percentage, "50" );
        TraceProvider traceProvider = getTraceProvider( config );
        Set<TransactionInitializationTrace> traces = new HashSet<>();
        for ( int i = 0; i < 100; i++ )
        {
            TransactionInitializationTrace traceInfo = traceProvider.getTraceInfo();
            traces.add( traceInfo );
        }
        assertTrue( traces.contains( NONE ) );
        assertTrue( traces.size() > 1 );
    }

    @Test
    void allTransactionsTracerCreation()
    {
        Config config = Config.defaults( GraphDatabaseSettings.transaction_tracing_level, ALL.name() );
        TraceProvider traceProvider = getTraceProvider( config );
        for ( int i = 0; i < 100; i++ )
        {
            assertNotEquals( NONE, traceProvider.getTraceInfo() );
        }
    }
}
