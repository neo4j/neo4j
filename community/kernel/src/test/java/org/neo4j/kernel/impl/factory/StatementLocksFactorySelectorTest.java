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
package org.neo4j.kernel.impl.factory;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.Locks.Client;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatementLocksFactorySelectorTest
{
    @Test
    public void loadSimpleStatementLocksFactoryWhenNoServices()
    {
        Locks locks = mock( Locks.class );
        Locks.Client locksClient = mock( Client.class );
        when( locks.newClient() ).thenReturn( locksClient );

        StatementLocksFactorySelector loader = newLoader( locks );

        StatementLocksFactory factory = loader.select();
        StatementLocks statementLocks = factory.newInstance();

        assertThat( factory, instanceOf( SimpleStatementLocksFactory.class ) );
        assertThat( statementLocks, instanceOf( SimpleStatementLocks.class ) );

        assertSame( locksClient, statementLocks.optimistic() );
        assertSame( locksClient, statementLocks.pessimistic() );
    }

    @Test
    public void loadSingleAvailableFactory()
    {
        Locks locks = mock( Locks.class );
        StatementLocksFactory factory = mock( StatementLocksFactory.class );

        StatementLocksFactorySelector loader = newLoader( locks, factory );

        StatementLocksFactory loadedFactory = loader.select();

        assertSame( factory, loadedFactory );
        verify( factory ).initialize( same( locks ), any( Config.class ) );
    }

    @Test
    public void throwWhenMultipleFactoriesLoaded()
    {
        TestStatementLocksFactorySelector loader = newLoader( mock( Locks.class ),
                mock( StatementLocksFactory.class ),
                mock( StatementLocksFactory.class ),
                mock( StatementLocksFactory.class ) );

        try
        {
            loader.select();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    private static TestStatementLocksFactorySelector newLoader( Locks locks, StatementLocksFactory... factories )
    {
        return new TestStatementLocksFactorySelector( locks, new Config(), NullLogService.getInstance(), factories );
    }

    private static class TestStatementLocksFactorySelector extends StatementLocksFactorySelector
    {
        private final List<StatementLocksFactory> factories;

        TestStatementLocksFactorySelector( Locks locks, Config config, LogService logService,
                StatementLocksFactory... factories )
        {
            super( locks, config, logService );
            this.factories = Arrays.asList( factories );
        }

        @Override
        List<StatementLocksFactory> serviceLoadFactories()
        {
            return factories;
        }
    }
}
