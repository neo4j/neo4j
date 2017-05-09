/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class LabelScanStoreLoggingTest
{

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void noLuceneLabelScanStoreMonitorMessages() throws Throwable
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        File storeDir = testDirectory.directory();
        GraphDatabaseService database = new TestGraphDatabaseFactory()
                        .setInternalLogProvider( logProvider )
                        .newEmbeddedDatabase( storeDir );
        try
        {

            DependencyResolver resolver = ((GraphDatabaseAPI) database).getDependencyResolver();

            NativeLabelScanStore labelScanStore = resolver.resolveDependency( NativeLabelScanStore.class );
            try ( LabelScanWriter labelScanWriter = labelScanStore.newWriter() )
            {
                labelScanWriter.write( NodeLabelUpdate.labelChanges( 1, new long[]{}, new long[]{1} ) );
            }
            labelScanStore.stop();
            labelScanStore.shutdown();

            labelScanStore.init();
            labelScanStore.start();

            logProvider.assertNoLogCallContaining( LuceneLabelScanStore.class.getName() );
            logProvider.assertContainsLogCallContaining( NativeLabelScanStore.class.getName() );
            logProvider.assertContainsMessageContaining(
                    "Scan store recovery completed: Number of cleaned crashed pointers" );
        }
        finally
        {
            database.shutdown();
        }
    }
}
