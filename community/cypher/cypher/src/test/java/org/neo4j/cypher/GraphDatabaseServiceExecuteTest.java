/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.IteratorUtil.count;

public class GraphDatabaseServiceExecuteTest
{
    @Test
    public void shouldExecuteCypher() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final int before, after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = count( GlobalGraphOperations.at( graphDb ).getAllNodes() );
            tx.success();
        }

        // when
        graphDb.execute( "CREATE (n:Foo{bar:\"baz\"})" );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = count( GlobalGraphOperations.at( graphDb ).getAllNodes() );
            tx.success();
        }
        assertEquals( before + 1, after );
    }
}
