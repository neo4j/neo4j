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
package org.neo4j.kernel.api.impl.insight;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;

public class InsightLuceneIndexUpdaterTest
{
    @ClassRule
    public static DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @ClassRule
    public static TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule( testDirectory.graphDbDir() );

    private static final Label LABEL = Label.label( "label1" );

    @Test
    public void shouldFindNodeWithString() throws IOException
    {
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();
        InsightIndex insightIndex = new InsightIndex( fileSystemRule, testDirectory.graphDbDir(), new int[]{1} );
        db.registerTransactionEventHandler( insightIndex.getUpdater() );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL );
            node.setProperty( "prop", "Hello. Hello again." );
            Node node2 = db.createNode( LABEL );
            node2.setProperty( "prop",
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any cross " +
                    "between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }

        InsightIndexReader reader = insightIndex.getReader();

        assertEquals( 0, reader.query( "hello" ).next() );
        assertEquals( 1, reader.query( "zebra" ).next() );
        assertEquals( 1, reader.query( "zedonk" ).next() );
        assertEquals( 1, reader.query( "cross" ).next() );

    }
}
