/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.function.Consumer;

import static org.junit.Assert.assertEquals;

public class AbstractCheckPointThresholdTest
{
    @Test
    public void shouldCallConsumerProvidingTheDescriptionWhenThresholdIsTrue() throws Throwable
    {
        // Given
        String description = "description";
        AbstractCheckPointThreshold threshold = new TheAbstractCheckPointThreshold( true, description );

        final AtomicReference<String> calledWith = new AtomicReference<>();
        // When
        threshold.isCheckPointingNeeded( 42, new Consumer<String>()
        {
            @Override
            public void accept( String description )
            {
                calledWith.set( description );
            }
        } );

        // Then
        assertEquals( description, calledWith.get() );
    }

    @Test
    public void shouldNotCallConsumerProvidingTheDescriptionWhenThresholdIsFalse() throws Throwable
    {
        // Given
        AbstractCheckPointThreshold threshold = new TheAbstractCheckPointThreshold( false, null );

        // When
        threshold.isCheckPointingNeeded( 42, new Consumer<String>()
        {
            @Override
            public void accept( String s )
            {
                throw new IllegalStateException( "nooooooooo!" );
            }
        } );

        // Then
        // should not throw
    }

    private static class TheAbstractCheckPointThreshold extends AbstractCheckPointThreshold
    {
        private final boolean reached;
        private final String description;

        public TheAbstractCheckPointThreshold( boolean reached, String description )
        {
            this.reached = reached;
            this.description = description;
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
        protected boolean thresholdReached( long lastCommittedTransactionId )
        {
            return reached;
        }

        @Override
        protected String description()
        {
            return description;
        }
    }
}
