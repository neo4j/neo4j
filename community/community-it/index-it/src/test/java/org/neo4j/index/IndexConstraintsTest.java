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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.internal.helpers.collection.Iterables.single;

class IndexConstraintsTest
{
    private static final Label LABEL = Label.label( "Label" );
    private static final String PROPERTY_KEY = "x";

    private GraphDatabaseService graphDb;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        graphDb = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void shutdown()
    {
        managementService.shutdown();
    }

    // The following tests verify that multiple interacting schema commands can be applied in the same transaction.

    @Test
    void convertIndexToConstraint()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create();
            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            IndexDefinition index = firstOrNull( tx.schema().getIndexes( LABEL ) );
            index.drop();

            tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.commit();
        }
        // assert no exception is thrown
    }

    @Test
    void convertIndexToConstraintWithExistingData()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < 2000; i++ )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( PROPERTY_KEY, i );
            }
            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create();
            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            IndexDefinition index = firstOrNull( tx.schema().getIndexes( LABEL ) );
            index.drop();

            tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.commit();
        }
        // assert no exception is thrown
    }

    @Test
    void convertConstraintToIndex()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create();
            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            ConstraintDefinition constraint = firstOrNull( tx.schema().getConstraints( LABEL ) );
            constraint.drop();

            tx.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create();
            tx.commit();
        }
        // assert no exception is thrown
    }

    @Test
    void creatingAndDroppingAndCreatingIndexInSameTransaction()
    {
        // go increasingly meaner
        for ( int times = 1; times <= 4; times++ )
        {
            try
            {
                // when: CREATE, DROP, CREATE => effect: CREATE
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( tx, tx.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create(), times );
                    tx.commit();
                }
                // then
                try ( Transaction tx = graphDb.beginTx() )
                {
                    assertNotNull( getIndex( tx, LABEL, PROPERTY_KEY ), "Index should exist" );
                }

                // when: DROP, CREATE => effect: <none>
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( tx, getIndex( tx, LABEL, PROPERTY_KEY ), times );
                    tx.commit();
                }
                // then
                try ( Transaction tx = graphDb.beginTx() )
                {
                    assertNotNull( getIndex( tx, LABEL, PROPERTY_KEY ), "Index should exist" );
                }

                // when: DROP, CREATE, DROP => effect: DROP
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( tx, getIndex( tx, LABEL, PROPERTY_KEY ), times ).drop();
                    tx.commit();
                }
                // then
                try ( Transaction tx = graphDb.beginTx() )
                {
                    assertNull( getIndex( tx, LABEL, PROPERTY_KEY ), "Index should be removed" );
                }
            }
            catch ( Throwable e )
            {
                throw new AssertionError( "times=" + times, e );
            }
        }
    }

    private IndexDefinition recreate( Transaction tx, IndexDefinition index, int times )
    {
        for ( int i = 0; i < times; i++ )
        {
            index.drop();
            index = tx.schema()
                    .indexFor( single( index.getLabels() ) )
                    .on( single( index.getPropertyKeys() ) )
                    .create();
        }
        return index;
    }

    private IndexDefinition getIndex( Transaction tx, Label label, String propertyKey )
    {
        IndexDefinition found = null;
        for ( IndexDefinition index : tx.schema().getIndexes( label ) )
        {
            if ( propertyKey.equals( single( index.getPropertyKeys() ) ) )
            {
                assertNull( found, "Found multiple indexes." );
                found = index;
            }
        }
        return found;
    }

    @Test
    void shouldRemoveIndexForConstraintEvenIfDroppedInCreatingTransaction()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // given
            tx.schema()
                    .constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY )
                    .create()
                    .drop();
            // when - rolling back
            tx.rollback();
        }
        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            assertNull( getIndex( tx, LABEL, PROPERTY_KEY ), "Should not have constraint index" );
        }
    }

    @Test
    void creatingAndDroppingAndCreatingConstraintInSameTransaction()
    {
        // go increasingly meaner
        for ( int times = 1; times <= 4; times++ )
        {
            try
            {
                // when: CREATE, DROP, CREATE => effect: CREATE
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( tx, tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( PROPERTY_KEY ).create(), times );
                    tx.commit();
                }
                // then
                try ( Transaction tx = graphDb.beginTx() )
                {
                    assertNotNull( getConstraint( tx, LABEL, PROPERTY_KEY ), "Constraint should exist" );
                    assertNotNull( getIndex( tx, LABEL, PROPERTY_KEY ), "Should have constraint index" );
                }

                // when: DROP, CREATE => effect: <none>
                try ( Transaction tx = graphDb.beginTx() )
                {
                    ConstraintDefinition constraint = getConstraint( tx, LABEL, PROPERTY_KEY );
                    recreate( tx, constraint, times );
                    tx.commit();
                }

                try ( Transaction tx = graphDb.beginTx() )
                {
                    // then
                    assertNotNull( getConstraint( tx, LABEL, PROPERTY_KEY ), "Constraint should exist" );
                    assertNotNull( getIndex( tx, LABEL, PROPERTY_KEY ), "Should have constraint index" );
                }

                // when: DROP, CREATE, DROP => effect: DROP
                try ( Transaction tx = graphDb.beginTx() )
                {
                    recreate( tx, getConstraint( tx, LABEL, PROPERTY_KEY ), times ).drop();
                    tx.commit();
                }
                try ( Transaction tx = graphDb.beginTx() )
                {
                    // then
                    assertNull( getConstraint( tx, LABEL, PROPERTY_KEY ), "Constraint should be removed" );
                    assertNull( getIndex( tx, LABEL, PROPERTY_KEY ), "Should not have constraint index" );
                }
            }
            catch ( Throwable e )
            {
                throw new AssertionError( "times=" + times, e );
            }
        }
    }

    private ConstraintDefinition recreate( Transaction tx, ConstraintDefinition constraint, int times )
    {
        for ( int i = 0; i < times; i++ )
        {
            constraint.drop();
            constraint = tx.schema()
                    .constraintFor( constraint.getLabel() )
                    .assertPropertyIsUnique( single( constraint.getPropertyKeys() ) )
                    .create();
        }
        return constraint;
    }

    private ConstraintDefinition getConstraint( Transaction tx, Label label, String propertyKey )
    {
        ConstraintDefinition found = null;
        for ( ConstraintDefinition constraint : tx.schema().getConstraints( label ) )
        {
            if ( propertyKey.equals( single( constraint.getPropertyKeys() ) ) )
            {
                assertNull( found, "Found multiple constraints." );
                found = constraint;
            }
        }
        return found;
    }
}
