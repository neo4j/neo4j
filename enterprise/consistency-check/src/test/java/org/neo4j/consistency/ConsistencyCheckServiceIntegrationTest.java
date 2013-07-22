/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.consistency;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.GraphStoreFixture;
import org.neo4j.test.TargetDirectory;

public class ConsistencyCheckServiceIntegrationTest
{
    @Test
    public void shouldProduceNoLogFileIfStoreIsConsistent() throws Exception
    {
        // given
        ConsistencyCheckService service = new ConsistencyCheckService();

        // when
        service.runFullConsistencyCheck( fixture.directory().getPath(),
                new Config( stringMap(  ), GraphDatabaseSettings.class, ConsistencyCheckSettings.class ), ProgressMonitorFactory.NONE, StringLogger.DEV_NULL );

        // then
        File reportFile = new File( fixture.directory(), service.defaultLogFileName() );
        assertFalse( "Inconsistency report file " + reportFile + " not generated", reportFile.exists() );
    }

    @Test
    public void shouldWriteInconsistenciesToLogFileInStoreDirectory() throws Exception
    {
        // given
        breakNodeStore();
        ConsistencyCheckService service = new ConsistencyCheckService();

        // when
        service.runFullConsistencyCheck( fixture.directory().getPath(),
                new Config( stringMap(  ), GraphDatabaseSettings.class, ConsistencyCheckSettings.class ), ProgressMonitorFactory.NONE, StringLogger.DEV_NULL );

        // then
        File reportFile = new File(fixture.directory(), service.defaultLogFileName());
        assertTrue( "Inconsistency report file " + reportFile + " not generated", reportFile.exists() );
    }

    @Test
    public void shouldWriteInconsistenciesToLogFileAtSpecifiedLocation() throws Exception
    {
        // given
        breakNodeStore();
        ConsistencyCheckService service = new ConsistencyCheckService();
        File specificLogFile = new File( testDirectory.directory(), "specific_logfile.txt" );

        // when
        service.runFullConsistencyCheck( fixture.directory().getPath(),
                new Config( stringMap( ConsistencyCheckSettings.consistency_check_report_file.name(),specificLogFile.getPath()),
                        GraphDatabaseSettings.class, ConsistencyCheckSettings.class ),
                ProgressMonitorFactory.NONE, StringLogger.DEV_NULL );

        // then
        assertTrue( "Inconsistency report file " + specificLogFile + " not generated", specificLogFile.exists() );
    }

    private void breakNodeStore() throws IOException
    {
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), next.relationship(), -1 ) );
            }
        } );
    }

    @Rule
    public final GraphStoreFixture fixture = new GraphStoreFixture()
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            org.neo4j.graphdb.Transaction tx = graphDb.beginTx();
            try
            {
                Node node1 = set( graphDb.createNode() );
                Node node2 = set( graphDb.createNode(), property( "key", "value" ) );
                node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "C" ) );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    };

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
}
