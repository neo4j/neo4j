/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.graphdb.aligned;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionLongToString;
import static org.neo4j.kernel.impl.store.format.StoreVersion.ALIGNED_V4_1;
import static org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_1.NAME;

@DbmsExtension( configurationCallback = "configure" )
public class AlignedRecordFormatIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private StoreIdProvider storeIdProvider;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.record_format, NAME );
    }

    @Test
    void databaseCanBeStartedWithAlignedFormat()
    {
        assertEquals( ALIGNED_V4_1.versionString(), versionLongToString( storeIdProvider.getStoreId().getStoreVersion() ) );
    }

    @Test
    void nodeAndRelationshipTransaction()
    {
        try ( var transaction = database.beginTx() )
        {
            var source = transaction.createNode( label( "marker" ) );
            var target = transaction.createNode();
            source.createRelationshipTo( target, withName( "link" ) );
            transaction.commit();
        }

        try ( var transaction = database.beginTx() )
        {
            assertEquals( 2, count( transaction.getAllNodes() ) );
            assertEquals( 1, count( transaction.getAllRelationships() ) );
            assertEquals( 1, count( transaction.getAllLabels() ) );
            assertEquals( 1, count( transaction.getAllRelationshipTypes() ) );
        }
    }

    @Test
    void nodesWithIndexedProperties()
    {
        var indexLabel = label( "indexMarker" );
        var propertyName = "property";
        var value = "value";
        var nodesCount = 100;
        try ( var transaction = database.beginTx() )
        {
            for ( int i = 0; i < nodesCount; i++ )
            {
                var node = transaction.createNode( indexLabel );
                node.setProperty( propertyName, value );
            }
            transaction.commit();
        }

        try ( var tx = database.beginTx() )
        {
            tx.schema().indexFor( indexLabel ).on( propertyName ).create();
            tx.commit();
        }

        try ( var tx = database.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, HOURS );
        }

        try ( var transaction = database.beginTx() )
        {
            assertEquals( nodesCount, count( transaction.findNodes( indexLabel, propertyName, value ) ) );
        }
    }

    @Test
    void nodesWithBigStringProperties()
    {
        var label = label( "marker" );
        var propertyName = "property";
        var nodesCount = 10;
        try ( var transaction = database.beginTx() )
        {
            for ( int i = 0; i < nodesCount; i++ )
            {
                var node = transaction.createNode( label );
                node.setProperty( propertyName, RandomStringUtils.randomAlphabetic( (int) mebiBytes( 1 ) ) );
            }
            transaction.commit();
        }

        try ( var transaction = database.beginTx() )
        {
            assertEquals( nodesCount, count( transaction.findNodes( label ) ) );
        }
    }

    @Test
    void nodesWithBigArrayProperties()
    {
        var label = label( "marker" );
        var propertyName = "property";
        var nodesCount = 10;
        try ( var transaction = database.beginTx() )
        {
            for ( int i = 0; i < nodesCount; i++ )
            {
                var node = transaction.createNode( label );
                node.setProperty( propertyName, RandomStringUtils.randomAlphabetic( (int) mebiBytes( 1 ) ).toCharArray() );
            }
            transaction.commit();
        }

        try ( var transaction = database.beginTx() )
        {
            assertEquals( nodesCount, count( transaction.findNodes( label ) ) );
        }
    }

}
