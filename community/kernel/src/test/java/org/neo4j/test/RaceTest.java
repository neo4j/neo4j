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
package org.neo4j.test;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.neo4j.concurrent.Runnables;

import static java.lang.Thread.sleep;
import static java.util.concurrent.ThreadLocalRandom.current;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.test.Race.throwing;

/**
 * Test of a test utility {@link Race}.
 */
public class RaceTest
{
    @Test
    public void shouldWaitForAllContestantsToComplete() throws Throwable
    {
        // GIVEN
        Race race = new Race();
        final AtomicInteger completed = new AtomicInteger();
        int count = 5;
        race.addContestants( count, throwing( () ->
        {
            sleep( current().nextInt( 100 ) );
            completed.incrementAndGet();
        } ) );

        // WHEN
        race.go();

        // THEN
        assertEquals( count, completed.get() );
    }

    @Test
    public void shouldConsultEndCondition() throws Throwable
    {
        // GIVEN
        CallCountBooleanSupplier endCondition = new CallCountBooleanSupplier( 100 );
        Race race = new Race().withEndCondition( endCondition );
        race.addContestants( 20, throwing( () -> sleep( 10 ) ) );

        // WHEN
        race.go();

        // THEN
        assertTrue( endCondition.callCount.get() >= 100 );
    }

    @Test
    public void shouldHaveMultipleEndConditions() throws Throwable
    {
        // GIVEN
        ControlledBooleanSupplier endCondition1 = spy( new ControlledBooleanSupplier( false ) );
        ControlledBooleanSupplier endCondition2 = spy( new ControlledBooleanSupplier( false ) );
        ControlledBooleanSupplier endCondition3 = spy( new ControlledBooleanSupplier( false ) );
        Race race = new Race().withEndCondition( endCondition1, endCondition2, endCondition3 );
        race.addContestant( () -> endCondition2.set( true ) );
        race.addContestants( 3, Runnables.EMPTY_RUNNABLE );

        // WHEN
        race.go();

        // THEN
        verify( endCondition1, atLeast( 4 ) ).getAsBoolean();
        verify( endCondition2, atLeast( 4 ) ).getAsBoolean();
    }

    @Test
    public void shouldBreakOnError() throws Throwable
    {
        // GIVEN
        String error = "Noooo";
        Race race = new Race();
        race.withEndCondition( () -> false ); // <-- never end
        race.addContestant( () ->
        {
            throw new RuntimeException( error );
        } );
        race.addContestants( 3, () ->
        {
        } );

        // WHEN
        try
        {
            race.go();
            fail( "Should've failed ");
        }
        catch ( Exception e )
        {
            // THEN
            assertEquals( error, e.getMessage() );
        }
    }

    public static class ControlledBooleanSupplier implements BooleanSupplier
    {
        private volatile boolean value;

        public ControlledBooleanSupplier( boolean initialValue )
        {
            this.value = initialValue;
        }

        public void set( boolean value )
        {
            this.value = value;
        }

        @Override
        public boolean getAsBoolean()
        {
            return value;
        }
    }

    public static class CallCountBooleanSupplier implements BooleanSupplier
    {
        private final int callCountTriggeringTrueEndCondition;
        private final AtomicInteger callCount = new AtomicInteger();

        public CallCountBooleanSupplier( int callCountTriggeringTrueEndCondition )
        {
            this.callCountTriggeringTrueEndCondition = callCountTriggeringTrueEndCondition;
        }

        @Override
        public boolean getAsBoolean()
        {
            return callCount.incrementAndGet() >= callCountTriggeringTrueEndCondition;
        }
    }
}
