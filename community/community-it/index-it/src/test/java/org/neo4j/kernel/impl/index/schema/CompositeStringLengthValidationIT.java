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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.index.internal.gbptree.TreeNodeDynamicSize;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith( RandomExtension.class )
@DbmsExtension( configurationCallback = "config" )
class CompositeStringLengthValidationIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";
    private static final String KEY2 = "key2";

    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private RandomRule random;

    private int firstSlotLength;
    private int secondSlotLength;

    @ExtensionCallback
    void config( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.default_schema_provider, GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerName() );
    }

    @BeforeEach
    void calculateSlotSizes()
    {
        int totalSpace = TreeNodeDynamicSize.keyValueSizeCapFromPageSize( PageCache.PAGE_SIZE ) - GenericKey.ENTITY_ID_SIZE;
        int perSlotOverhead = GenericKey.TYPE_ID_SIZE + GenericKey.SIZE_STRING_LENGTH;
        int firstSlotSpace = totalSpace / 2;
        int secondSlotSpace = totalSpace - firstSlotSpace;
        this.firstSlotLength = firstSlotSpace - perSlotOverhead;
        this.secondSlotLength = secondSlotSpace - perSlotOverhead;
    }

    @Test
    void shouldHandleCompositeSizesCloseToTheLimit() throws KernelException
    {
        String firstSlot = random.nextAlphaNumericString( firstSlotLength, firstSlotLength );
        String secondSlot = random.nextAlphaNumericString( secondSlotLength, secondSlotLength );

        // given
        createIndex( KEY, KEY2 );

        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode( LABEL );
            node.setProperty( KEY, firstSlot );
            node.setProperty( KEY2, secondSlot );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx =
                    db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                            .getKernelTransactionBoundToThisThread( true, db.databaseId() );
            int labelId = ktx.tokenRead().nodeLabel( LABEL.name() );
            int propertyKeyId1 = ktx.tokenRead().propertyKey( KEY );
            int propertyKeyId2 = ktx.tokenRead().propertyKey( KEY2 );
            try ( NodeValueIndexCursor cursor = ktx.cursors().allocateNodeValueIndexCursor() )
            {
                IndexReadSession index = ktx.dataRead().indexReadSession(
                        TestIndexDescriptorFactory.forLabel( labelId, propertyKeyId1, propertyKeyId2 ) );
                ktx.dataRead().nodeIndexSeek( index,
                                              cursor, IndexOrder.NONE, false, IndexQuery.exact( propertyKeyId1, firstSlot ),
                                              IndexQuery.exact( propertyKeyId2, secondSlot ) );
                assertTrue( cursor.next() );
                assertEquals( node.getId(), cursor.nodeReference() );
                assertFalse( cursor.next() );
            }
            tx.commit();
        }
    }

    @Test
    void shouldFailBeforeCommitOnCompositeSizesLargerThanLimit()
    {
        String firstSlot = random.nextAlphaNumericString( firstSlotLength + 1, firstSlotLength + 1 );
        String secondSlot = random.nextAlphaNumericString( secondSlotLength, secondSlotLength );

        // given
        createIndex( KEY, KEY2 );

        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( LABEL );
                node.setProperty( KEY, firstSlot );
                node.setProperty( KEY2, secondSlot );
                tx.commit();
            }
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
            assertThat( e.getMessage(),
                    containsString( "Property value is too large to index into this particular index. Please see index documentation for limitations. " ) );
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
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.commit();
        }
    }
}
