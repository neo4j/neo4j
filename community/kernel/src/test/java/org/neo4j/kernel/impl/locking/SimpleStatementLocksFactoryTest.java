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
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleStatementLocksFactoryTest
{
    @Test
    public void throwsForNullLocks()
    {
        try
        {
            new SimpleStatementLocksFactory( null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void createSimpleStatementLocks()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        SimpleStatementLocksFactory factory = new SimpleStatementLocksFactory( locks );

        StatementLocks statementLocks = factory.newInstance();

        assertThat( statementLocks, instanceOf( SimpleStatementLocks.class ) );
        assertSame( client, statementLocks.optimistic() );
        assertSame( client, statementLocks.pessimistic() );
    }

    @Test
    public void newInstanceThrowsWhenNotInitialized()
    {
        SimpleStatementLocksFactory factory = new SimpleStatementLocksFactory();
        try
        {
            factory.newInstance();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void initializeThrowsForNullLocks()
    {
        SimpleStatementLocksFactory factory = new SimpleStatementLocksFactory();
        try
        {
            factory.initialize( null, new Config() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }
}
