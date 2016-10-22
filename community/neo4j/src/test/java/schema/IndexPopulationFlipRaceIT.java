/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package schema;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

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
        KernelAPI kernelAPI = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        try ( KernelTransaction tx = kernelAPI.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() );
              Statement statement = tx.acquireStatement() )
        {
            int labelAId = statement.readOperations().labelGetForName( labelA( i ).name() );
            int keyAId = statement.readOperations().propertyKeyGetForName( keyA( i ) );
            int labelBId = statement.readOperations().labelGetForName( labelB( i ).name() );
            int keyBId = statement.readOperations().propertyKeyGetForName( keyB( i ) );

            for ( int j = 0; j < NODES_PER_INDEX; j++ )
            {
                long nodeAId = data.first()[j];
                assertEquals( 1, statement.readOperations().nodesCountIndexed(
                        new IndexDescriptor( labelAId, keyAId ), nodeAId, nodeAId ) );
                long nodeBId = data.other()[j];
                assertEquals( 1, statement.readOperations().nodesCountIndexed(
                        new IndexDescriptor( labelBId, keyBId ), nodeBId, nodeBId ) );
            }
        }
    }
}
