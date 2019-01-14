/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.locking.DeferringStatementLocksFactory.deferred_locks_enabled;

public class DeferringStatementLocksFactoryTest
{
    @Test
    public void initializeThrowsForNullLocks()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        try
        {
            factory.initialize( null, Config.defaults() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void initializeThrowsForNullConfig()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        try
        {
            factory.initialize( mock( Locks.class ), null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void newInstanceThrowsWhenNotInitialized()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
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
    public void newInstanceCreatesSimpleLocksWhenConfigNotSet()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        Config config = Config.defaults( deferred_locks_enabled, Settings.FALSE );

        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        factory.initialize( locks, config );

        StatementLocks statementLocks = factory.newInstance();

        assertThat( statementLocks, instanceOf( SimpleStatementLocks.class ) );
        assertSame( client, statementLocks.optimistic() );
        assertSame( client, statementLocks.pessimistic() );
    }

    @Test
    public void newInstanceCreatesDeferredLocksWhenConfigSet()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        Config config = Config.defaults( deferred_locks_enabled, Settings.TRUE );

        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        factory.initialize( locks, config );

        StatementLocks statementLocks = factory.newInstance();

        assertThat( statementLocks, instanceOf( DeferringStatementLocks.class ) );
        assertThat( statementLocks.optimistic(), instanceOf( DeferringLockClient.class ) );
        assertSame( client, statementLocks.pessimistic() );
    }
}
