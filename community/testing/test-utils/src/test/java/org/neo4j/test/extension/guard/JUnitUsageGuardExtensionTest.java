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
package org.neo4j.test.extension.guard;

import org.junit.jupiter.api.Test;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

class JUnitUsageGuardExtensionTest
{
    @Test
    void detectIncorrectAssertUsage()
    {
        Events testEvents = executeTest( IncorrectAssertUsage.class );

        verifyFailureMessage( testEvents, "Detected Junit 4 classes: [org.junit.Assert]" );
    }

    @Test
    void detectIncorrectIgnoreNewTest()
    {
        Events testEvents = executeTest( IgnoreNewTestWithOldAnnotation.class );

        verifyFailureMessage( testEvents, "Detected Junit 4 classes: [org.junit.Ignore]" );
    }

    @Test
    void detectMixtureOfDifferentTests()
    {
        Events testEvents = executeTest( MixOfDifferentJUnits.class );

        verifyFailureMessage( testEvents, "Detected Junit 4 classes: [org.junit.Test]" );
    }

    @Test
    void deleteOldRuleAndNewTest()
    {
        Events testEvents = executeTest( MixRuleAndNewJUnit.class );

        verifyFailureMessage( testEvents, "Detected Junit 4 classes: [org.junit.Rule]" );
    }

    @Test
    void validTestUsage()
    {
        Events testEvents = executeTest( ValidUsage.class );
        assertEquals( 0, testEvents.failed().count() );
    }

    private void verifyFailureMessage( Events testEvents, String expectedMessage )
    {
        testEvents.assertThatEvents().haveExactly( 1,
                event( finishedWithFailure( instanceOf( JUnitException.class ), message( message -> message.contains( expectedMessage ) ) ) ) );
    }

    private Events executeTest( Class clazz )
    {
        return EngineTestKit.engine( ENGINE_ID )
                .selectors( selectClass( clazz ) ).execute()
                .all();
    }
}
