/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Integer.max;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.test.TestLabels.LABEL_ONE;

public class GenericIndexValidationIT
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule().withSetting( default_schema_provider, NATIVE_BTREE10.providerIdentifier() );

    @Rule
    public final RandomRule random = new RandomRule();

    private static final String propKey = "largeString";
    private static final int keySizeLimit = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE ) - Long.BYTES;

    @Test
    public void shouldSuccessfullyWriteAndReadWithinIndexKeySizeLimit()
    {
        createIndex();
        String[] propValue = generateValue( keySizeLimit - 100 /*because there are some internal overhead of e.g. array values*/ );
        long expectedNodeId;

        // Write
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( LABEL_ONE );
            node.setProperty( propKey, propValue );
            expectedNodeId = node.getId();
            tx.success();
        }

        // Read
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.findNode( LABEL_ONE, propKey, propValue );
            assertNotNull( node );
            assertEquals( "node id", expectedNodeId, node.getId() );
            tx.success();
        }
    }

    @Test
    public void txMustFailIfExceedingIndexKeySizeLimit()
    {
        createIndex();

        // Write
        try ( Transaction tx = db.beginTx() )
        {
            String[] propValue = generateValue( keySizeLimit + 1 /*just to be on the safe side*/ );
            db.createNode( LABEL_ONE ).setProperty( propKey, propValue );
            tx.success();
        }
        catch ( IllegalArgumentException e )
        {
            assertThat( e.getMessage(),
                    Matchers.containsString( "is too large to index into this particular index. Please see index documentation for limitations." ) );
        }
    }

    // Each char in string need to fit in one byte
    private String[] generateValue( int byteArraySize )
    {
        String[] array = new String[random.nextInt( 2, 5 )];
        int usedSize = 0;
        for ( int i = 0; i < array.length; i++ )
        {
            array[i] = random.nextAlphaNumericString( 1, max( 1, byteArraySize - usedSize ) );
            usedSize += array[i].length();
        }
        return array;
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( propKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }
}
