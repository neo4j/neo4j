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
package org.neo4j.metatest;

import org.junit.Test;
import org.junit.runners.model.Statement;
import org.mockito.InOrder;

import org.neo4j.test.CleanupRule;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestCleanupRule
{
    @Test
    public void shouldCleanupAutoCloseable() throws Throwable
    {
        // GIVEN
        CleanupRule rule = new CleanupRule();
        AutoCloseable toClose = rule.add( mock( AutoCloseable.class ) );

        // WHEN
        simulateTestExecution( rule );

        // THEN
        verify( toClose ).close();
    }

    @Test
    public void shouldCleanupObjectWithAppropriateCloseMethod() throws Throwable
    {
        // GIVEN
        CleanupRule rule = new CleanupRule();
        Dirt toClose = rule.add( mock( Dirt.class ) );

        // WHEN
        simulateTestExecution( rule );

        // THEN
        verify( toClose ).shutdown();
    }

    @Test
    public void shouldCleanupMultipleObjectsInReverseAddedOrder() throws Throwable
    {
        // GIVEN
        CleanupRule rule = new CleanupRule();
        AutoCloseable closeable = rule.add( mock( AutoCloseable.class ) );
        Dirt dirt = rule.add( mock( Dirt.class ) );

        // WHEN
        simulateTestExecution( rule );

        // THEN
        InOrder inOrder = inOrder( dirt, closeable );
        inOrder.verify( dirt, times( 1 ) ).shutdown();
        inOrder.verify( closeable, times( 1 ) ).close();
    }

    @Test
    public void shouldTellUserIllegalArgumentIfSo() throws Throwable
    {
        // GIVEN
        CleanupRule rule = new CleanupRule();
        try
        {
            rule.add( new Object() );
            fail( "Should not accept this object" );
        }
        catch ( IllegalArgumentException e )
        {   // OK, good
        }
    }

    private void simulateTestExecution( CleanupRule rule ) throws Throwable
    {
        rule.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
            }
        }, null ).evaluate();
    }

    private interface Dirt
    {
        void shutdown();
    }
}
