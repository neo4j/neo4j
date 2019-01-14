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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class AbstractCheckPointThresholdTest
{
    @Test
    public void shouldCallConsumerProvidingTheDescriptionWhenThresholdIsTrue()
    {
        // Given
        String description = "description";
        AbstractCheckPointThreshold threshold = new TheAbstractCheckPointThreshold( true, description );

        final AtomicReference<String> calledWith = new AtomicReference<>();
        // When
        threshold.isCheckPointingNeeded( 42, calledWith::set );

        // Then
        assertEquals( description, calledWith.get() );
    }

    @Test
    public void shouldNotCallConsumerProvidingTheDescriptionWhenThresholdIsFalse()
    {
        // Given
        AbstractCheckPointThreshold threshold = new TheAbstractCheckPointThreshold( false, null );

        // When
        threshold.isCheckPointingNeeded( 42, s ->
        {
            throw new IllegalStateException( "nooooooooo!" );
        } );

        // Then
        // should not throw
    }

    private static class TheAbstractCheckPointThreshold extends AbstractCheckPointThreshold
    {
        private final boolean reached;

        TheAbstractCheckPointThreshold( boolean reached, String description )
        {
            super( description );
            this.reached = reached;
        }

        @Override
        public void initialize( long transactionId )
        {

        }

        @Override
        public void checkPointHappened( long transactionId )
        {

        }

        @Override
        public long checkFrequencyMillis()
        {
            return DEFAULT_CHECKING_FREQUENCY_MILLIS;
        }

        @Override
        protected boolean thresholdReached( long lastCommittedTransactionId )
        {
            return reached;
        }
    }
}
