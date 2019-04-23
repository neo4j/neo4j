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
package org.neo4j.monitoring;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class CompositeDatabaseHealthTest
{

    @Test
    void shouldPanicAllDatabasesTogether()
    {
        // given
        List<DatabaseHealth> dbHealths = Stream.generate( CompositeDatabaseHealthTest::mockDBHealth ).limit( 5 ).collect( Collectors.toList() );
        CompositeDatabaseHealth compositeDatabaseHealth = new CompositeDatabaseHealth( dbHealths );

        // when
        Throwable expectedCause = new Exception( "Everybody panic!" );
        compositeDatabaseHealth.panic( expectedCause );

        // then
        for ( DatabaseHealth dbHealth : dbHealths )
        {
            assertFalse( dbHealth.isHealthy() );
            assertEquals( expectedCause, dbHealth.cause() );
        }
    }

    @Test
    void shouldAssertHealthyOnAllDatabasesTogether()
    {
        // given
        List<DatabaseHealth> dbHealths = Stream.generate( CompositeDatabaseHealthTest::mockDBHealth ).limit( 5 ).collect( Collectors.toList() );
        CompositeDatabaseHealth compositeDatabaseHealth = new CompositeDatabaseHealth( dbHealths );

        // when
        compositeDatabaseHealth.assertHealthy( IllegalStateException.class );

        // then
        for ( DatabaseHealth dbHealth : dbHealths )
        {
            verify( dbHealth ).assertHealthy( eq( IllegalStateException.class ) );
        }
    }

    @Test
    void shouldSuppressMultipleCauses()
    {
        // given
        int numUnhealthyDBs = 5;
        List<DatabaseHealth> unhealthyDBs = Stream.generate( CompositeDatabaseHealthTest::mockDBHealth )
                .peek( dbHealth -> dbHealth.panic( new Exception( "Error" ) ) )
                .limit( numUnhealthyDBs ).collect( Collectors.toList() );
        DatabaseHealth healthyDB = mockDBHealth();
        unhealthyDBs.add( healthyDB );
        CompositeDatabaseHealth compositeDatabaseHealth = new CompositeDatabaseHealth( unhealthyDBs );

        // then
        assertFalse( compositeDatabaseHealth.isHealthy() );
        Throwable compositeCause = compositeDatabaseHealth.cause();
        assertThat( compositeCause.getMessage(), containsString( "Some of the databases have panicked" ) );
        Throwable[] suppressed = compositeCause.getSuppressed();
        assertEquals( numUnhealthyDBs, suppressed.length );
    }

    //TODO: Test that a databaseHealth removes itself when stopped.

    private static DatabaseHealth mockDBHealth()
    {
        DatabasePanicEventGenerator generator = mock( DatabasePanicEventGenerator.class );
        return spy( new DatabaseHealth( generator, NullLogProvider.getInstance().getLog( DatabaseHealth.class ) ) );
    }

}
