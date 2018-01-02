/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TargetDirectory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Just testing the {@link BatchInserter} in an enterprise setting, i.e. with all packages and extensions
 * that exist in enterprise edition.
 */
public class BatchInsertEnterpriseTest
{
    private enum Labels implements Label
    {
        One,
        Two;
    }

    @Rule
    public final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldInsertDifferentTypesOfThings() throws Exception
    {
        // GIVEN
        BatchInserter inserter = BatchInserters.inserter( directory.directory(), stringMap(
                GraphDatabaseSettings.log_queries.name(), "true",
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

    private Map<String,Object> someProperties( int id )
    {
        return map( "key", "value" + id, "number", 10 + id );
    }
}
