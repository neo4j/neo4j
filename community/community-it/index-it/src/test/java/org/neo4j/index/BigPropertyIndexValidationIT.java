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
package org.neo4j.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.mockito.matcher.Neo4jMatchers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ImpermanentDbmsExtension
class BigPropertyIndexValidationIT
{
    @Inject
    private GraphDatabaseService db;

    private Label LABEL;
    private String longString;
    private String propertyKey;

    @BeforeEach
    void setup()
    {
        LABEL = Label.label( "LABEL" );
        char[] chars = new char[1 << 15];
        Arrays.fill( chars, 'c' );
        longString = new String( chars );
        propertyKey = "name";
    }

    @Test
    void shouldFailTransactionThatIndexesLargePropertyDuringNodeCreation()
    {
        // GIVEN
        IndexDefinition index = Neo4jMatchers.createIndex( db, LABEL, propertyKey );

        //We expect this transaction to fail due to the huge property
        assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                assertThrows( IllegalArgumentException.class, () -> db.execute( "CREATE (n:" + LABEL + " {name: \"" + longString + "\"})" ) );
                tx.commit();
            }
            //Check that the database is empty.
            try ( Transaction tx = db.beginTx() )
            {
                ResourceIterator<Node> nodes = db.getAllNodes().iterator();
                assertFalse( nodes.hasNext() );
            }
        } );
    }

    @Test
    void shouldFailTransactionThatIndexesLargePropertyAfterNodeCreation()
    {
        // GIVEN
        IndexDefinition index = Neo4jMatchers.createIndex( db, LABEL, propertyKey );

        //We expect this transaction to fail due to the huge property
        assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.execute( "CREATE (n:" + LABEL + ")" );
                assertThrows( IllegalArgumentException.class, () -> db.execute( "match (n:" + LABEL + ")set n.name= \"" + longString + "\"" ) );
                tx.commit();
            }
            //Check that the database is empty.
            try ( Transaction tx = db.beginTx() )
            {
                ResourceIterator<Node> nodes = db.getAllNodes().iterator();
                assertFalse( nodes.hasNext() );
            }
        } );
    }

    @Test
    void shouldFailTransactionThatIndexesLargePropertyOnLabelAdd()
    {
        // GIVEN
        IndexDefinition index = Neo4jMatchers.createIndex( db, LABEL, propertyKey );

        //We expect this transaction to fail due to the huge property
        assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                String otherLabel = "SomethingElse";
                db.execute( "CREATE (n:" + otherLabel + " {name: \"" + longString + "\"})" );
                assertThrows( IllegalArgumentException.class, () -> db.execute( "match (n:" + otherLabel + ")set n:" + LABEL ) );
                tx.commit();
            }
            //Check that the database is empty.
            try ( Transaction tx = db.beginTx() )
            {
                ResourceIterator<Node> nodes = db.getAllNodes().iterator();
                assertFalse( nodes.hasNext() );
            }
        } );
    }
}
