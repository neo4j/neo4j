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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.common.ProgressReporter;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.time.FakeClock;

import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

class VisibleMigrationProgressMonitorTest
{
    @Test
    void shouldReportAllPercentageSteps()
    {
        // GIVEN
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        VisibleMigrationProgressMonitor monitor = new VisibleMigrationProgressMonitor( log );
        monitor.started( 1 );

        // WHEN
        monitorSection( monitor, "First", 100, 40, 25, 23 /*these are too far*/ , 10, 50 );
        monitor.completed();

        // THEN
        verifySectionReportedCorrectly( logProvider );
    }

    @Test
    void progressNeverReportMoreThenHundredPercent()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        VisibleMigrationProgressMonitor monitor = new VisibleMigrationProgressMonitor( log );

        monitor.started( 1 );
        monitorSection( monitor, "First", 100, 1, 10, 99, 170 );
        monitor.completed();

        verifySectionReportedCorrectly( logProvider );
    }

    @Test
    void reportStartStopOfTransactionLogsMigration()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        VisibleMigrationProgressMonitor monitor = new VisibleMigrationProgressMonitor( log );

        monitor.startTransactionLogsMigration();
        monitor.completeTransactionLogsMigration();

        assertThat( logProvider ).containsMessages( VisibleMigrationProgressMonitor.TX_LOGS_MIGRATION_STARTED,
                VisibleMigrationProgressMonitor.TX_LOGS_MIGRATION_COMPLETED );
    }

    @Test
    void shouldIncludeDurationInCompletionMessage()
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        FakeClock clock = new FakeClock();
        VisibleMigrationProgressMonitor monitor = new VisibleMigrationProgressMonitor( log, clock );

        // when
        monitor.started( 1 );
        clock.forward( 1500, TimeUnit.MILLISECONDS );
        monitor.completed();

        // then
        assertThat( logProvider ).containsMessages( "took 1s 500ms" );
    }

    private void verifySectionReportedCorrectly( AssertableLogProvider logProvider )
    {
        var messageMatcher = assertThat( logProvider );
        messageMatcher.containsMessages( VisibleMigrationProgressMonitor.MESSAGE_STARTED );
        for ( int i = 10; i <= 100; i += 10 )
        {
            messageMatcher.containsMessages( i + "%" );
        }
        messageMatcher.containsMessages( VisibleMigrationProgressMonitor.MESSAGE_COMPLETED );
        messageMatcher.forClass( VisibleMigrationProgressMonitor.class ).forLevel( INFO ).doesNotContainMessage( "110%" );
    }

    private void monitorSection( VisibleMigrationProgressMonitor monitor, String name, int max, int... steps )
    {
        ProgressReporter progressReporter = monitor.startSection( name );
        progressReporter.start( max );
        for ( int step : steps )
        {
            progressReporter.progress( step );
        }
        progressReporter.completed();
    }
}
