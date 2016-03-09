/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.Test;

import org.neo4j.bolt.v1.runtime.integration.RecordingCallback;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.v1.runtime.Session.Callback.noOp;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.ignored;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.recorded;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.success;
import static org.neo4j.helpers.collection.MapUtil.map;

public class SessionStateMachineResetTest
{
    @Test
    public void shouldKillMessagesAheadInLine() throws Throwable
    {
        // Given
        RecordingCallback recorder = new RecordingCallback();
        SessionStateMachine ssm = new SessionStateMachine( mock( SessionStateMachine.SPI.class ) );
        ssm.init( "bob/1.0", map(), null, noOp() );

        // When
        ssm.interrupt();
        
        // Then
        ssm.run( "hello", map(), null, recorder );
        ssm.reset( null, recorder );
        ssm.run( "hello", map(), null, recorder );
        assertThat( recorder, recorded(
            ignored(),
            success(),
            success()
        ));
    }

    @Test
    public void multipleInterruptsShouldBeMatchedWithMultipleResets() throws Throwable
    {
        // Given
        RecordingCallback recorder = new RecordingCallback();
        SessionStateMachine ssm = new SessionStateMachine( mock( SessionStateMachine.SPI.class ) );
        ssm.init( "bob/1.0", map(), null, noOp() );

        // When
        ssm.interrupt();
        ssm.interrupt();

        // Then
        ssm.run( "hello", map(), null, recorder );
        ssm.reset( null, recorder );
        ssm.run( "hello", map(), null, recorder );
        assertThat( recorder, recorded(
                ignored(),
                ignored(),
                ignored()
        ));

        recorder = new RecordingCallback(); // to get a clean recording

        // But when
        ssm.reset( null, recorder );

        // Then
        ssm.run( "hello", map(), null, recorder );
        assertThat( recorder, recorded(
                success(),
                success()
        ));
    }
}
