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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.mockito.matcher.Neo4jMatchers;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

@EnableRuleMigrationSupport
@ExtendWith( ImpermanentDatabaseExtension.class )
public class BigPropertyIndexValidationIT
{
    @Resource
    public ImpermanentDatabaseRule dbRule;
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private Label LABEL;
    private String longString;
    private String propertyKey;

    @BeforeEach
    public void setup()
    {
        LABEL = Label.label( "LABEL" );
        char[] chars = new char[1 << 15];
        Arrays.fill( chars, 'c' );
        longString = new String( chars );
        propertyKey = "name";
    }

    @Test
    public void shouldFailTransactionThatIndexesLargePropertyDuringNodeCreation()
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        IndexDefinition index = Neo4jMatchers.createIndex( db, LABEL, propertyKey );

        //We expect this transaction to fail due to the huge property
        expectedException.expect( TransactionFailureException.class );
        try ( Transaction tx = db.beginTx() )
        {
            try
            {
                db.execute( "CREATE (n:" + LABEL + " {name: \"" + longString + "\"})" );
                fail( "Argument was illegal" );
            }
            catch ( IllegalArgumentException e )
            {
                //this is expected.
            }
            tx.success();
        }
        //Check that the database is empty.
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> nodes = db.getAllNodes().iterator();
            assertFalse( nodes.hasNext() );
        }
        db.shutdown();
    }

    @Test
    public void shouldFailTransactionThatIndexesLargePropertyAfterNodeCreation()
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        IndexDefinition index = Neo4jMatchers.createIndex( db, LABEL, propertyKey );

        //We expect this transaction to fail due to the huge property
        expectedException.expect( TransactionFailureException.class );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CREATE (n:" + LABEL + ")" );
            try
            {
                db.execute( "match (n:" + LABEL + ")set n.name= \"" + longString + "\"" );
                fail( "Argument was illegal" );
            }
            catch ( IllegalArgumentException e )
            {
                //this is expected.
            }
            tx.success();
        }
        //Check that the database is empty.
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> nodes = db.getAllNodes().iterator();
            assertFalse( nodes.hasNext() );
        }
        db.shutdown();
    }

    @Test
    public void shouldFailTransactionThatIndexesLargePropertyOnLabelAdd()
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        IndexDefinition index = Neo4jMatchers.createIndex( db, LABEL, propertyKey );

        //We expect this transaction to fail due to the huge property
        expectedException.expect( TransactionFailureException.class );
        try ( Transaction tx = db.beginTx() )
        {
            String otherLabel = "SomethingElse";
            db.execute( "CREATE (n:" + otherLabel + " {name: \"" + longString + "\"})" );
            try
            {
                db.execute( "match (n:" + otherLabel + ")set n:" + LABEL );
                fail( "Argument was illegal" );
            }
            catch ( IllegalArgumentException e )
            {
                //this is expected.
            }
            tx.success();
        }
        //Check that the database is empty.
        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> nodes = db.getAllNodes().iterator();
            assertFalse( nodes.hasNext() );
        }
        db.shutdown();
    }
}
