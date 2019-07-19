/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.test.extension.timeout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.testkit.engine.EngineTestKit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class VerboseExceptionExtensionTest
{
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void dumpThreadDumpOnTestTimeout()
    {
        executeTest( DumpThreadDumpOnTimeout.class );
        assertTrue( suppressOutput.getErrorVoice().containsMessage( "Thread dump" ) );
    }

    @Test
    void doNotDumpThreadDumpOnTestAssumptionFailure()
    {
        executeTest( DoNotDumpThreadsOnAssumptionFailure.class );
        assertFalse( suppressOutput.getErrorVoice().containsMessage( "Thread dump" ) );
    }

    private static void executeTest( Class<?> clazz )
    {
        EngineTestKit.engine( ENGINE_ID )
                .selectors( selectClass( clazz ) ).execute()
                .all();
    }
}
