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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static java.time.LocalDate.now;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.test.TestLabels.LABEL_ONE;

public class IndexValueSupportIT
{
    private static final String KEY = "key";

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule().startLazily();

    @Test
    public void shouldFailOnIndexingTemporalValueInUnsupportedIndex()
    {
        shouldFailOnIndexingValueInUnsupportedIndex( now() );
    }

    @Test
    public void shouldFailOnIndexingSpatialValueInUnsupportedIndex()
    {
        PointValue value = Values.pointValue( CoordinateReferenceSystem.WGS84, 2.0, 2.0 );
        shouldFailOnIndexingValueInUnsupportedIndex( value );
    }

    private void shouldFailOnIndexingValueInUnsupportedIndex( Object value )
    {
        // given
        db.ensureStarted( GraphDatabaseSettings.enable_native_schema_index.name(), "false" );
        createAndAwaitIndex();

        // when
        try
        {
            createNodeWithProperty( value );
            fail( "Should have failed" );
        }
        catch ( TransactionFailureException e )
        {
            // then good
        }
    }

    @Test
    public void shouldSucceedOnIndexingTemporalValueInSupportedIndex()
    {
        shouldSucceedOnIndexingTemporalValueInSupportedIndex( now() );
    }

    @Test
    public void shouldSucceedOnIndexingSpatialValueInSupportedIndex()
    {
        PointValue value = Values.pointValue( CoordinateReferenceSystem.WGS84, 2.0, 2.0 );
        shouldSucceedOnIndexingTemporalValueInSupportedIndex( value );
    }

    private void shouldSucceedOnIndexingTemporalValueInSupportedIndex( Object value )
    {
        // given
        db.ensureStarted( GraphDatabaseSettings.enable_native_schema_index.name(), "true" );
        createAndAwaitIndex();

        // when
        Node node = createNodeWithProperty( value );

        // then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( node, db.findNode( LABEL_ONE, KEY, value ) );
            tx.success();
        }
    }

    private Node createNodeWithProperty( Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL_ONE );
            node.setProperty( KEY, value );
            tx.success();
            return node;
        }
    }

    private void createAndAwaitIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }
}
