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
package schema;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

public class IndexPopulationFlipRaceIT
{
    private static final int NODES_PER_INDEX = 10;

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldAtomicallyFlipMultipleIndexes() throws Exception
    {
        // A couple of times since this is probabilistic, but also because there seems to be a difference
        // in timings between the first time and all others... which is perhaps super obvious to some, but not to me.
        for ( int i = 0; i < 10; i++ )
        {
            // GIVEN
            createIndexesButDontWaitForThemToFullyPopulate( i );

            // WHEN
            Pair<long[],long[]> data = createDataThatGoesIntoToThoseIndexes( i );

            // THEN
            awaitIndexes();
            verifyThatThereAreExactlyOneIndexEntryPerNodeInTheIndexes( i, data );
        }
    }

    private void awaitIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }

    private void createIndexesButDontWaitForThemToFullyPopulate( int i )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( labelA( i ) ).on( keyA( i ) ).create();

            if ( random.nextBoolean() )
            {
                db.schema().indexFor( labelB( i ) ).on( keyB( i ) ).create();
            }
            else
            {
                db.schema().constraintFor( labelB( i ) ).assertPropertyIsUnique( keyB( i ) ).create();
            }
            tx.success();
        }
    }

    private String keyB( int i )
    {
        return "key_b" + i;
    }

    private Label labelB( int i )
    {
        return label( "Label_b" + i );
    }

    private String keyA( int i )
    {
        return "key_a" + i;
    }

    private Label labelA( int i )
    {
        return label( "Label_a" + i );
    }

    private Pair<long[],long[]> createDataThatGoesIntoToThoseIndexes( int i )
    {
        long[] dataA = new long[NODES_PER_INDEX];
        long[] dataB = new long[NODES_PER_INDEX];
        for ( int t = 0; t < NODES_PER_INDEX; t++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node nodeA = db.createNode( labelA( i ) );
                nodeA.setProperty( keyA( i ), dataA[t] = nodeA.getId() );
                Node nodeB = db.createNode( labelB( i ) );
                nodeB.setProperty( keyB( i ), dataB[t] = nodeB.getId() );
                tx.success();
            }
        }
        return Pair.of( dataA, dataB );
    }

    private void verifyThatThereAreExactlyOneIndexEntryPerNodeInTheIndexes( int i, Pair<long[],long[]> data )
            throws Exception
    {
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( Session session = kernel.beginSession( AnonymousContext.read() );
              org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            int labelAId = tx.tokenRead().nodeLabel( labelA( i ).name() );
            int keyAId = tx.tokenRead().propertyKey( keyA( i ) );
            int labelBId = tx.tokenRead().nodeLabel( labelB( i ).name() );
            int keyBId = tx.tokenRead().propertyKey( keyB( i ) );
            IndexReference indexA = DefaultIndexReference.general( labelAId, keyAId );
            IndexReference indexB = DefaultIndexReference.general( labelBId, keyBId );

            for ( int j = 0; j < NODES_PER_INDEX; j++ )
            {
                long nodeAId = data.first()[j];
                assertEquals( 1, tx.schemaRead().nodesCountIndexed(
                        indexA, nodeAId, Values.of( nodeAId ) ) );
                long nodeBId = data.other()[j];
                assertEquals( 1, tx.schemaRead().nodesCountIndexed(
                        indexB, nodeBId, Values.of( nodeBId ) ) );
            }
        }
    }
}
