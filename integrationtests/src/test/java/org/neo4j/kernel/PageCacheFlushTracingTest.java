/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class PageCacheFlushTracingTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void tracePageCacheFlushProgress()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        GraphDatabaseService database = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider )
                                            .newEmbeddedDatabaseBuilder( testDirectory.directory() )
                                            .setConfig( GraphDatabaseFacadeFactory.Configuration.tracer, "verbose" )
                                            .newGraphDatabase();
        try ( Transaction transaction = database.beginTx() )
        {
            database.createNode();
            transaction.success();
        }
        database.shutdown();
        logProvider.assertContainsMessageContaining( "Flushing file" );
        logProvider.assertContainsMessageContaining( "Page cache flush completed. Flushed " );
    }
}
