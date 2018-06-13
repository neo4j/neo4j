/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime;

import org.junit.Test;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.udc.UsageData;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BoltFactoryImplTest
{
    private static final Clock CLOCK = Clock.systemUTC();
    private static final BoltChannel CHANNEL = mock( BoltChannel.class );

    @Test
    public void shouldCreateBoltStateMachines()
    {
        BoltFactoryImpl factory = newBoltFactory();

        BoltStateMachine boltStateMachine = factory.newMachine( CHANNEL, CLOCK );

        assertNotNull( boltStateMachine );
    }

    private static BoltFactoryImpl newBoltFactory()
    {
        return newBoltFactory( newDbMock() );
    }

    private static BoltFactoryImpl newBoltFactory( GraphDatabaseAPI db )
    {
        return new BoltFactoryImpl( db, new UsageData( new OnDemandJobScheduler() ), new AvailabilityGuard( CLOCK, NullLog.getInstance() ),
                mock( Authentication.class ), BoltConnectionTracker.NOOP, Config.defaults(), NullLogService.getInstance() );
    }

    private static GraphDatabaseAPI newDbMock()
    {
        GraphDatabaseAPI db = mock( GraphDatabaseAPI.class );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( db.getDependencyResolver() ).thenReturn( dependencyResolver );
        GraphDatabaseQueryService queryService = mock( GraphDatabaseQueryService.class );
        when( queryService.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ) ).thenReturn( queryService );
        return db;
    }
}
