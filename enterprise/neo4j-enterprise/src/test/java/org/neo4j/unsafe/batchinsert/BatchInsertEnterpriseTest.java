/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.batchinsert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0_7;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Just testing the {@link BatchInserter} in an enterprise setting, i.e. with all packages and extensions
 * that exist in enterprise edition.
 */
@RunWith( Parameterized.class )
public class BatchInsertEnterpriseTest
{
    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( StandardV3_0_7.NAME, HighLimit.NAME );
    }

    @Test
    public void shouldInsertDifferentTypesOfThings() throws Exception
    {
        // GIVEN
        BatchInserter inserter = BatchInserters.inserter( directory.directory(), stringMap(
                GraphDatabaseSettings.log_queries.name(), "true",
                GraphDatabaseSettings.record_format.name(), recordFormat,
                GraphDatabaseSettings.log_queries_filename.name(), directory.file( "query.log" ).getAbsolutePath() ) );
        long node1Id, node2Id, relationshipId;
        try
        {
            // WHEN
            node1Id = inserter.createNode( someProperties( 1 ), Labels.values() );
            node2Id = node1Id + 10;
            inserter.createNode( node2Id, someProperties( 2 ), Labels.values() );
            relationshipId = inserter.createRelationship( node1Id, node2Id, MyRelTypes.TEST, someProperties( 3 ) );
            inserter.createDeferredSchemaIndex( Labels.One ).on( "key" ).create();
            inserter.createDeferredConstraint( Labels.Two ).assertPropertyIsUnique( "key" ).create();
        }
        finally
        {
            inserter.shutdown();
        }

        // THEN
        GraphDatabaseService db = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( directory.directory() );
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.getNodeById( node1Id );
            Node node2 = db.getNodeById( node2Id );
            assertEquals( someProperties( 1 ), node1.getAllProperties() );
            assertEquals( someProperties( 2 ), node2.getAllProperties() );
            assertEquals( relationshipId, single( node1.getRelationships() ).getId() );
            assertEquals( relationshipId, single( node2.getRelationships() ).getId() );
            assertEquals( someProperties( 3 ), single( node1.getRelationships() ).getAllProperties() );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void insertIntoExistingDatabase() throws IOException
    {
        File storeDir = directory.directory();

        GraphDatabaseService db = newDb( storeDir, recordFormat );
        try
        {
            createThreeNodes( db );
        }
        finally
        {
            db.shutdown();
        }

        BatchInserter inserter = BatchInserters.inserter( storeDir );
        try
        {
            long start = inserter.createNode( someProperties( 5 ), Labels.One );
            long end = inserter.createNode( someProperties( 5 ), Labels.One );
            inserter.createRelationship( start, end, MyRelTypes.TEST, someProperties( 5 ) );
        }
        finally
        {
            inserter.shutdown();
        }

        db = newDb( storeDir, recordFormat );
        try
        {
            verifyNodeCount( db, 4 );
        }
        finally
        {
            db.shutdown();
        }
    }

    private static void verifyNodeCount( GraphDatabaseService db, int expectedNodeCount )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( expectedNodeCount, Iterables.count( db.getAllNodes() ) );
            tx.success();
        }
    }

    private static void createThreeNodes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( Labels.One );
            someProperties( 5 ).forEach( start::setProperty );

            Node end = db.createNode( Labels.Two );
            someProperties( 5 ).forEach( end::setProperty );

            Relationship rel = start.createRelationshipTo( end, MyRelTypes.TEST );
            someProperties( 5 ).forEach( rel::setProperty );

            tx.success();
        }
    }

    private static Map<String,Object> someProperties( int id )
    {
        return map( "key", "value" + id, "number", 10 + id );
    }

    private GraphDatabaseService newDb( File storeDir, String recordFormat )
    {
        return new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                .newGraphDatabase();
    }

    private enum Labels implements Label
    {
        One,
        Two
    }
}
