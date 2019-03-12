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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

@ExtendWith( LifeExtension.class )
class LifeExtensionTest
{
    @Inject
    private LifeSupport lifecycle;

    @Test
    void extensionInjectSupportingLifecycle()
    {
        assertNotNull( lifecycle );
    }

    @Test
    void injectedLifeIsStartedAndStartingAddedComponents()
    {
        TestComponent testComponent = new TestComponent();
        lifecycle.add( testComponent );
        assertTrue( testComponent.isStarted() );
    }

    @Test
    void componentShutdownAfterTest()
    {
        Events testEvents = EngineTestKit.engine( ENGINE_ID )
                .selectors( selectClass( LifeExtensionComponentShutdownCase.class ) ).execute()
                .tests();

        testEvents.assertThatEvents().haveExactly( 1,
                event( finishedWithFailure( instanceOf( RuntimeException.class ),
                        message( message -> message.contains( "Shutdown exception." ) ) ) ) );
    }

    @Test
    void incorrectLifeSupportExtensionUsageTest()
    {
        Events testEvents = EngineTestKit.engine( ENGINE_ID )
                .selectors( selectClass( LifeExtensionIncorrectUsage.class ) ).execute()
                .tests();

        testEvents.assertThatEvents().haveExactly( 1,
                event( finishedWithFailure( instanceOf( ExtensionConfigurationException.class ),
                        message( message -> message.contains( "Field lifeSupport that is marked for injection" ) ) ) ) );
    }

    private static class TestComponent extends LifecycleAdapter
    {
        private boolean started;

        @Override
        public void start() throws Exception
        {
            super.start();
            started = true;
        }

        public boolean isStarted()
        {
            return started;
        }
    }
}
