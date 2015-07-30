/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestLabels;
import org.neo4j.test.Unzip;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class ReadMultipleLogEntryVersionsTest
{
    @Ignore( "Run in 2.2.2 to create the database" )
    @Test
    public void shouldCreateSomeStuffAndCrashAs222() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(
                TargetDirectory.forTest( getClass() ).makeGraphDbDir().getAbsolutePath() );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode( TestLabels.LABEL_ONE );
            Node node2 = db.createNode();
            node2.setProperty( "key", "some very nice value" );
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            tx.success();
        }

        System.exit( 1 );
    }

    @Test
    public void shouldRecoveryThatDbAs224() throws Exception
    {
        // GIVEN
        File path = Unzip.unzip( getClass(), "2.2.2-crashed-db.zip" );
        StoreRecoverer recoverer = new StoreRecoverer();
        assertTrue( recoverer.recoveryNeededAt( path ) );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path.getAbsolutePath() );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 2, count( GlobalGraphOperations.at( db ).getAllNodes() ) );
            assertEquals( 1, count( GlobalGraphOperations.at( db ).getAllRelationships() ) );
            tx.success();
        }
    }
}
