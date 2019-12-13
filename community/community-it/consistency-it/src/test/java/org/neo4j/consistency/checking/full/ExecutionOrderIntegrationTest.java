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
package org.neo4j.consistency.checking.full;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.newchecker.NodeBasedMemoryLimiter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

@PageCacheExtension
@ExtendWith( SuppressOutputExtension.class )
public class ExecutionOrderIntegrationTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;

    private GraphStoreFixture fixture;

    @BeforeEach
    void setUp()
    {
        fixture = new GraphStoreFixture( getRecordFormatName(), pageCache, testDirectory )
        {
            @Override
            protected void generateInitialData( GraphDatabaseService graphDb )
            {
                try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
                {
                    Node node1 = set( tx.createNode( label( "Foo" ) ) );
                    Node node2 = set( tx.createNode( label( "Foo" ) ), property( "key", "value" ) );
                    node1.createRelationshipTo( node2, RelationshipType.withName( "C" ) );
                    tx.commit();
                }
            }

            @Override
            protected Map<Setting<?>,Object> getConfig()
            {
                return getSettings();
            }
        };
    }

    @AfterEach
    void tearDown() throws Exception
    {
        fixture.close();
    }

    @Test
    void shouldRunChecksInSingleThreadedPass() throws Exception
    {
        // given
        int threads = defaultConsistencyCheckThreadsNumber();

        FullCheck singlePass =
                new FullCheck( ProgressMonitorFactory.NONE, Statistics.NONE, threads, ConsistencyFlags.DEFAULT, getTuningConfiguration(), false,
                        NodeBasedMemoryLimiter.DEFAULT );

        ConsistencySummaryStatistics singlePassSummary = new ConsistencySummaryStatistics();
        InconsistencyLogger logger = mock( InconsistencyLogger.class );

        // when
        singlePass.execute( fixture.getInstantiatedPageCache(), fixture.directStoreAccess(),
                new InconsistencyReport( logger, singlePassSummary ), fixture.counts().get() );

        // then
        verifyNoInteractions( logger );
        assertEquals( 0, singlePassSummary.getTotalInconsistencyCount(), "Expected no inconsistencies in single pass." );
    }

    private Config getTuningConfiguration()
    {
        return Config.defaults( Map.of( GraphDatabaseSettings.pagecache_memory, "8m",
                GraphDatabaseSettings.record_format, getRecordFormatName() ) );
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }

    protected Map<Setting<?>,Object> getSettings()
    {
        return new HashMap<>();
    }
}
