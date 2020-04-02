/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.index.label;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.common.EntityType;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.neo4j.logging.LogAssertions.assertThat;

class LoggingMonitorTest
{
    @ParameterizedTest
    @EnumSource
    void shouldAdaptMessageForEntityType( EntityType type )
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( LoggingMonitorTest.class );
        LoggingMonitor loggingMonitor = new LoggingMonitor( log, type );
        String lowerToken = type == EntityType.NODE ? "label" : "relationship type";
        String upperToken = type == EntityType.NODE ? "Label" : "Relationship type";
        String typePlural = type == EntityType.NODE ? "nodes" : "relationships";

        // When
        loggingMonitor.noIndex();
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( lowerToken );
        logProvider.clear();

        loggingMonitor.notValidIndex();
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( upperToken );
        logProvider.clear();

        loggingMonitor.rebuilding();
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( lowerToken );
        logProvider.clear();

        loggingMonitor.rebuilt( 0 );
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( upperToken, typePlural );
        logProvider.clear();

        loggingMonitor.recoveryCleanupRegistered();
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( upperToken );
        logProvider.clear();

        loggingMonitor.recoveryCleanupStarted();
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( upperToken );
        logProvider.clear();

        loggingMonitor.recoveryCleanupFinished( 0, 0, 0, 0 );
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( upperToken );
        logProvider.clear();

        loggingMonitor.recoveryCleanupClosed();
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( upperToken );
        logProvider.clear();

        loggingMonitor.recoveryCleanupFailed( new Exception() );
        assertThat( logProvider ).forClass( LoggingMonitorTest.class ).containsMessages( upperToken );
        logProvider.clear();
    }
}
