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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.Test;

import org.neo4j.common.ProgressReporter;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.hamcrest.Matchers.containsString;

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
    void reportStartStopOftransactionLogsMigration()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        VisibleMigrationProgressMonitor monitor = new VisibleMigrationProgressMonitor( log );

        monitor.startTransactionLogsMigration();
        monitor.completeTransactionLogsMigration();

        logProvider.assertContainsMessageContaining( VisibleMigrationProgressMonitor.TX_LOGS_MIGRATION_STARTED );
        logProvider.assertContainsMessageContaining( VisibleMigrationProgressMonitor.TX_LOGS_MIGRATION_COMPLETED );
    }

    private void verifySectionReportedCorrectly( AssertableLogProvider logProvider )
    {
        logProvider.assertContainsMessageContaining( VisibleMigrationProgressMonitor.MESSAGE_STARTED );
        for ( int i = 10; i <= 100; i += 10 )
        {
            logProvider.assertContainsMessageContaining( i + "%" );
        }
        logProvider.assertNone( AssertableLogProvider.inLog( VisibleMigrationProgressMonitor.class ).info( containsString( "110%" ) ) );
        logProvider.assertContainsMessageContaining( VisibleMigrationProgressMonitor.MESSAGE_COMPLETED );
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
