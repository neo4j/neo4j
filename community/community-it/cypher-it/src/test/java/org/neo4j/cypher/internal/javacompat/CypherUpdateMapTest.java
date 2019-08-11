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
package org.neo4j.cypher.internal.javacompat;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

class CypherUpdateMapTest
{
    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void cleanup()
    {
        managementService.shutdown();
    }

    @Test
    void updateNodeByMapParameter()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            db.execute( "CREATE (n:Reference) SET n = $data RETURN n", map( "data", map( "key1", "value1", "key2", 1234 ) ) ).close();
            transaction.commit();
        }

        Node node1 = getNodeByIdInTx( 0 );

        assertThat( node1, inTxS( hasProperty( "key1" ).withValue( "value1" ) ) );
        assertThat( node1, inTxS( hasProperty( "key2" ).withValue( 1234 ) ) );

        try ( Transaction transaction = db.beginTx() )
        {
            db.execute( "MATCH (n:Reference) SET n = $data RETURN n", map( "data", map( "key1", null, "key3", 5678 ) ) ).close();
            transaction.commit();
        }

        Node node2 = getNodeByIdInTx( 0 );

        assertThat( node2, inTxS( not( hasProperty( "key1" ) ) ) );
        assertThat( node2, inTxS( not( hasProperty( "key2" ) ) ) );
        assertThat( node2, inTxS( hasProperty( "key3" ).withValue( 5678 ) ) );
    }

    <T> Matcher<? super T> inTxS( final Matcher<T> inner )
    {
        return inTx( db, inner, false );
    }

    private Node getNodeByIdInTx( int nodeId )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return db.getNodeById( nodeId );
        }
    }
}
