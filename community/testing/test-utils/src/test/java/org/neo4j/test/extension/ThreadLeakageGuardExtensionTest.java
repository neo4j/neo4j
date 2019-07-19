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
package org.neo4j.test.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtensionContextException;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

@DisabledIfSystemProperty( named = "junit.jupiter.execution.parallel.mode.classes.default", matches = "concurrent" )
class ThreadLeakageGuardExtensionTest
{
    @Test
    void threadLeakageTest() throws InterruptedException
    {
        Events testEvents = EngineTestKit.engine( ENGINE_ID )
                .selectors( selectClass( IncorrectThreadLeakage.class ) )
                .execute()
                .all();

        testEvents.assertThatEvents().haveExactly( 1,
                event( finishedWithFailure( instanceOf( ExtensionContextException.class ),
                        message( message -> message.contains( "1 leaked thread(s) detected" ) ) ) ) );

        IncorrectThreadLeakage.cleanUp();
    }
}
