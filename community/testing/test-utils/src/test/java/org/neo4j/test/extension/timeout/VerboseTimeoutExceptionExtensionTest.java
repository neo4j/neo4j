/*
 * Copyright (c) "Neo4j"
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

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

import java.util.Arrays;

import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;

class VerboseTimeoutExceptionExtensionTest
{
    @Test
    void shouldDumpThreadsOnTimeout()
    {
        assertTestGetsThreadDump( "dumpOnTimeoutException" );
        assertTestGetsThreadDump( "dumpOnAssertionFailedErrorWithMessage" );
        assertTestGetsThreadDump( "dumpOnTimeoutPreemptively" );
        assertTestGetsThreadDump( "dumpOnTimeoutAnnotation" );
        assertTestGetsThreadDump( "dumpOnCauseTimeout" );
        assertTestGetsThreadDump( "dumpOnSuppressedTimeout" );
        assertTestGetsThreadDump( "dumpOnDeepCauseTimeout" );
        assertTestGetsThreadDump( "dumpOnDeepSuppressedTimeout" );
        assertTestGetsThreadDump( "dumpOnAssertEventually" );
    }

    @Test
    void shouldNotDumpThreadsOnNormalFailure()
    {
        assertTestGetsNoThreadDump( "doNotDumpOnAssume" );
        assertTestGetsNoThreadDump( "doNotDumpOnAssert" );
        assertTestGetsNoThreadDump( "doNotDumpOnException" );
        assertTestGetsNoThreadDump( "doNotDumpOnDeepException" );
    }

    void assertTestGetsThreadDump( String test )
    {
        assertThreadDumpEvent( executeTest( test ), true );
    }

    void assertTestGetsNoThreadDump( String test )
    {
        assertThreadDumpEvent( executeTest( test ), false );
    }

    private static Events executeTest( String method )
    {
        Events events = EngineTestKit.engine( ENGINE_ID ).selectors( selectMethod( DumpThreadDumpOnTimeout.class, method ) ).execute().testEvents();
        events.assertStatistics( stats -> stats.finished( 1 ) );
        return events;
    }

    private static void assertThreadDumpEvent( Events events, boolean shouldHave )
    {
        events.assertThatEvents().haveExactly( shouldHave ? 1 : 0,
                event( finishedWithFailure( suppressed( instanceOf( VerboseTimeoutExceptionExtension.ThreadDump.class ) ) ) ) );
    }

    private static Condition<Throwable> suppressed( Condition<Throwable> condition )
    {
        return new Condition<>( throwable -> Arrays.stream( throwable.getSuppressed() ).anyMatch( condition::matches ),
                "Suppressed throwable matches %s", condition );
    }
}
