/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.newchecker;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.DebugContext;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.experimental_consistency_checker;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.consistency.checking.full.ConsistencyFlags.DEFAULT;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@PageCacheExtension
class IndexCheckingSelectorTest
{
    private static final Label label1 = Label.label( "Label1" );
    private static final String property1 = "property1";
    private final StringJoiner output = new StringJoiner( System.lineSeparator() );
    private final DebugContext debugContext = new DebugContext()
    {
        @Override
        public boolean debugEnabled()
        {
            return true;
        }

        @Override
        public void debug( String message )
        {
            output.add( message );
        }
    };
    private GraphStoreFixture fixture;

    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DefaultFileSystemAbstraction fs;

    @AfterEach
    void tearDown() throws Exception
    {
        if ( fixture != null )
        {
            fixture.close();
        }
    }

    @Test
    void checkLargeNodeIndexesWithIndexChecker() throws Exception
    {
        fixture = new GraphStoreFixture( StringUtils.EMPTY, pageCache, testDirectory )
        {
            @Override
            protected void generateInitialData( GraphDatabaseService db )
            {
                // An index is considered large if it contains more than 5% of the nodes.
                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    Node node = tx.createNode( label1 );
                    node.setProperty( property1, "value" );
                    tx.commit();
                }

                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( label1 ).on( property1 ).create();
                    tx.commit();
                }

                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );

                    tx.commit();
                }
            }

            @Override
            protected Map<Setting<?>,Object> getConfig()
            {
                return MapUtil.genericMap( GraphDatabaseSettings.experimental_consistency_checker, true );
            }
        };

        runConsistencyCheck();

        String outputString = output.toString();
        assertThat( outputString, Matchers.containsString( "IndexChecker[entityType:NODE,indexesToCheck:1]" ) );
        assertThat( outputString, Matchers.containsString( "NodeChecker[highId:1,indexesToCheck:0]" ) );
    }

    @Test
    void checkSmallNodeIndexesWithNodeChecker() throws Exception
    {
        fixture = new GraphStoreFixture( StringUtils.EMPTY, pageCache, testDirectory )
        {
            @Override
            protected void generateInitialData( GraphDatabaseService db )
            {
                // An index is considered small if it contains less than 5% of the nodes.
                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    Node node = tx.createNode( label1 );
                    node.setProperty( property1, "value" );

                    for ( int i = 0; i < 20; i++ )
                    {
                        tx.createNode();
                    }
                    tx.commit();
                }

                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( label1 ).on( property1 ).create();
                    tx.commit();
                }

                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );

                    tx.commit();
                }
            }

            @Override
            protected Map<Setting<?>,Object> getConfig()
            {
                return MapUtil.genericMap( GraphDatabaseSettings.experimental_consistency_checker, true );
            }
        };

        runConsistencyCheck();

        String outputString = output.toString();
        assertThat( outputString, Matchers.containsString( "IndexChecker[entityType:NODE,indexesToCheck:0]" ) );
        assertThat( outputString, Matchers.containsString( "NodeChecker[highId:21,indexesToCheck:1]" ) );
    }

    @Test
    void checkIndexesWithoutValuesWithNodeChecker() throws Exception
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setConfig( GraphDatabaseSettings.experimental_consistency_checker, true )
                .build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label1 );
            node.setProperty( property1, "value" );
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.execute( "CALL db.index.fulltext.createNodeIndex('indexName', ['Label1'], ['property1']);" );
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        managementService.shutdown();

        DatabaseLayout layout = Neo4jLayout.of( testDirectory.homeDir() ).databaseLayout( DEFAULT_DATABASE_NAME );
        Config config = Config.defaults( GraphDatabaseSettings.logs_directory, layout.databaseDirectory().toPath() );
        config.set( experimental_consistency_checker, true );
        ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck( layout, config, ProgressMonitorFactory.NONE,
                nullLogProvider(), fs, pageCache, debugContext, layout.databaseDirectory(), DEFAULT );
        assertTrue( result.isSuccessful() );

        String outputString = output.toString();
        assertThat( outputString, Matchers.containsString( "IndexChecker[entityType:NODE,indexesToCheck:0]" ) );
        assertThat( outputString, Matchers.containsString(  "NodeChecker[highId:1,indexesToCheck:1]" ) );
    }

    private void runConsistencyCheck() throws ConsistencyCheckIncompleteException
    {
        ConsistencySummaryStatistics result = checkIndex();
        assertTrue( result.isConsistent() );
    }

    private ConsistencySummaryStatistics checkIndex() throws ConsistencyCheckIncompleteException
    {
        FullCheck checker = new FullCheck( ProgressMonitorFactory.NONE, Statistics.NONE, defaultConsistencyCheckThreadsNumber(), DEFAULT,
                Config.defaults( GraphDatabaseSettings.experimental_consistency_checker, true ), debugContext, NodeBasedMemoryLimiter.DEFAULT );
        return checker.execute( pageCache, fixture.readOnlyDirectStoreAccess(), fixture.counts(), NullLog.getInstance() );
    }
}
