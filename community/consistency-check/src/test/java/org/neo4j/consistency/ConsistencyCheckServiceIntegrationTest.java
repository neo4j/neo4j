/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.consistency.ConsistencyCheckService.Result;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.consistency.ConsistencyCheckService.defaultLogFileName;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class ConsistencyCheckServiceIntegrationTest
{
    private final GraphStoreFixture fixture = new GraphStoreFixture( getRecordFormatName() )
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
            {
                Node node1 = set( graphDb.createNode() );
                Node node2 = set( graphDb.createNode(), property( "key", "value" ) );
                node1.createRelationshipTo( node2, RelationshipType.withName( "C" ) );
                tx.success();
            }
        }
    };

    private final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public final RuleChain chain = RuleChain.outerRule( testDirectory ).around( fixture );

    @Test
    public void shouldSucceedIfStoreIsConsistent() throws Exception
    {
        // given
        Date timestamp = new Date();
        ConsistencyCheckService service = new ConsistencyCheckService( timestamp );
        Config configuration = new Config( settings(), GraphDatabaseSettings.class, ConsistencyCheckSettings.class );

        // when
        ConsistencyCheckService.Result result = runFullConsistencyCheck( service, configuration );

        // then
        assertEquals( ConsistencyCheckService.Result.SUCCESS, result );
        File reportFile = new File( fixture.directory(), defaultLogFileName( timestamp ) );
        assertFalse( "Unexpected generation of consistency check report file: " + reportFile, reportFile.exists() );
    }

    @Test
    public void shouldFailIfTheStoreInNotConsistent() throws Exception
    {
        // given
        breakNodeStore();
        Date timestamp = new Date();
        ConsistencyCheckService service = new ConsistencyCheckService( timestamp );
        Config configuration = new Config(
                settings( GraphDatabaseSettings.logs_directory.name(), testDirectory.directory().getPath() ),
                GraphDatabaseSettings.class, ConsistencyCheckSettings.class );

        // when
        ConsistencyCheckService.Result result = runFullConsistencyCheck( service, configuration );

        // then
        assertEquals( ConsistencyCheckService.Result.FAILURE, result );
        File reportFile = new File(
                configuration.get( GraphDatabaseSettings.logs_directory ),
                defaultLogFileName( timestamp ) );
        assertTrue( "Inconsistency report file " + reportFile + " not generated", reportFile.exists() );
    }

    @Test
    public void shouldNotReportDuplicateForHugeLongValues() throws Exception
    {
        // given
        ConsistencyCheckService service = new ConsistencyCheckService();
        Config configuration = new Config( settings(), GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( GraphDatabaseSettings.record_format, getRecordFormatName() )
                .newGraphDatabase();

        String propertyKey = "itemId";
        Label label = Label.label( "Item" );
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            set( db.createNode( label ), property( propertyKey, 973305894188596880L ) );
            set( db.createNode( label ), property( propertyKey, 973305894188596864L ) );
            tx.success();
        }
        db.shutdown();

        // when
        Result result = runFullConsistencyCheck( service, configuration );

        // then
        assertEquals( ConsistencyCheckService.Result.SUCCESS, result );
    }

    @Test
    public void shouldAllowGraphCheckDisabled() throws IOException, ConsistencyCheckIncompleteException
    {
        GraphDatabaseService gds = getGraphDatabaseService();

        try ( Transaction tx = gds.beginTx() )
        {
            gds.createNode();
            tx.success();
        }

        gds.shutdown();

        ConsistencyCheckService service = new ConsistencyCheckService();
        Config configuration = new Config(
                settings( ConsistencyCheckSettings.consistency_check_graph.name(), Settings.FALSE ),
                GraphDatabaseSettings.class, ConsistencyCheckSettings.class );

        // when
        Result result = runFullConsistencyCheck( service, configuration );

        // then
        assertEquals( ConsistencyCheckService.Result.SUCCESS, result );
    }

    private GraphDatabaseService getGraphDatabaseService()
    {
        GraphDatabaseBuilder builder =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.absolutePath() );
        builder.setConfig( settings(  ) );

        return builder.newGraphDatabase();
    }

    protected Map<String,String> settings( String... strings )
    {
        Map<String, String> defaults = new HashMap<>();
        defaults.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        defaults.put( GraphDatabaseSettings.record_format.name(), getRecordFormatName() );
        return stringMap( defaults, strings );
    }

    private void breakNodeStore() throws TransactionFailureException
    {
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                    GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), false, next.relationship(), -1 ) );
            }
        } );
    }

    private Result runFullConsistencyCheck( ConsistencyCheckService service, Config configuration )
            throws ConsistencyCheckIncompleteException, IOException
    {
        return service.runFullConsistencyCheck( fixture.directory(),
                configuration, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false );
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }
}
