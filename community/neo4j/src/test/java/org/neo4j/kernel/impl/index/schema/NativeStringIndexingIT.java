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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.Randoms.CSA_LETTERS_AND_DIGITS;

public class NativeStringIndexingIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";
    private static final String KEY2 = "key2";

    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldHandleSizesCloseToTheLimit()
    {
        // given
        createIndex( KEY );

        // when
        Map<String,Long> strings = new HashMap<>();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 1_000; i++ )
            {
                String string;
                do
                {
                    string = random.string( 3_000, 4_000, CSA_LETTERS_AND_DIGITS );
                }
                while ( strings.containsKey( string ) );

                Node node = db.createNode( LABEL );
                node.setProperty( KEY, string );
                strings.put( string, node.getId() );
            }
            tx.success();
        }

        // then
        try ( Transaction tx = db.beginTx() )
        {
            for ( String string : strings.keySet() )
            {
                Node node = db.findNode( LABEL, KEY, string );
                assertEquals( strings.get( string ).longValue(), node.getId() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldFailBeforeCommitOnSizesLargerThanLimit()
    {
        // given
        createIndex( KEY );

        // when a string slightly longer than the native string limit
        int length = 5_000;
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( LABEL ).setProperty( KEY, random.string( length, length, CSA_LETTERS_AND_DIGITS ) );
                tx.success();
            }
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
            assertThat( e.getMessage(), containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
        }
    }

    @Test
    public void shouldHandleCompositeSizesCloseToTheLimit() throws KernelException
    {
        // given
        createIndex( KEY, KEY2 );

        // when a string longer than native string limit, but within lucene limit
        int length = 20_000;
        String string1 = random.string( length, length, CSA_LETTERS_AND_DIGITS );
        String string2 = random.string( length, length, CSA_LETTERS_AND_DIGITS );
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( LABEL );
            node.setProperty( KEY, string1 );
            node.setProperty( KEY2, string2 );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx =
                    db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                            .getKernelTransactionBoundToThisThread( true );
            int labelId = ktx.tokenRead().nodeLabel( LABEL.name() );
            int propertyKeyId1 = ktx.tokenRead().propertyKey( KEY );
            int propertyKeyId2 = ktx.tokenRead().propertyKey( KEY2 );
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                ktx.dataRead().nodeIndexSeek( DefaultIndexReference.general( labelId, propertyKeyId1, propertyKeyId2 ),
                        cursor, IndexOrder.NONE,
                        IndexQuery.exact( propertyKeyId1, string1 ),
                        IndexQuery.exact( propertyKeyId2, string2 ) );
                assertTrue( cursor.next() );
                assertEquals( node.getId(), cursor.nodeReference() );
                assertFalse( cursor.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldFailBeforeCommitOnCompositeSizesLargerThanLimit()
    {
        // given
        createIndex( KEY, KEY2 );

        // when a string longer than lucene string limit
        int length = 50_000;
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                node.setProperty( KEY, random.string( length, length, CSA_LETTERS_AND_DIGITS ) );
                node.setProperty( KEY2, random.string( length, length, CSA_LETTERS_AND_DIGITS ) );
                tx.success();
            }
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
            assertThat( e.getMessage(), containsString( "Property value size is too large for index. Please see index documentation for limitations." ) );
        }
    }

    private void createIndex( String... keys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator indexCreator = db.schema().indexFor( LABEL );
            for ( String key : keys )
            {
                indexCreator = indexCreator.on( key );
            }
            indexCreator.create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }
    }
}
