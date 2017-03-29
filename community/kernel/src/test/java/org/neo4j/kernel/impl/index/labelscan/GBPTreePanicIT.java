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
package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.CheckpointCounter;
import org.neo4j.index.internal.gbptree.CountingMonitor;
import org.neo4j.kernel.Health;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GBPTreePanicIT
{
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory( GBPTreePanicIT.class, fileSystemRule.get() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( fileSystemRule ).around( pageCacheRule );

    private TestGraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory();
    private CountingMonitor checkpointCounter = new CheckpointCounter();
    private Label label = Label.label( "label" );

    @Before
    public void registerGBPTreeMonitor()
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( checkpointCounter );
        dbFactory.setMonitors( monitors );
    }

    @Test
    public void shouldNotCheckpointNativeLabelScanStoreOnCloseAfterPanic() throws Exception
    {
        // GIVEN
        GraphDatabaseService graphDb = createDb();
        Health health = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( Health.class );
        createNode( graphDb );

        // WHEN
        health.panic( new IOException( "fake" ) );
        checkpointCounter.reset();
        graphDb.shutdown();

        // THEN
        assertThat( checkpointCounter.count(), is( 0 ) );
    }

    private void createNode( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.createNode( label );
            tx.success();
        }
    }

    private GraphDatabaseService createDb()
    {
        return dbFactory.newImpermanentDatabase( testDirectory.graphDbDir() );
    }
}
