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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterables.count;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
class RelationshipGroupStoreIT
{
    private static final int RELATIONSHIP_COUNT = 20;

    @Inject
    private GraphDatabaseAPI db;

    @ExtensionCallback
    static void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.dense_node_threshold, 1 );
    }

    @Test
    void shouldCreateAllTheseRelationshipTypes()
    {
        shiftHighId( db );

        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < RELATIONSHIP_COUNT; i++ )
            {
                node.createRelationshipTo( db.createNode(), type( i ) );
            }
            tx.commit();
        }

        try ( Transaction ignored = db.beginTx() )
        {
            for ( int i = 0; i < RELATIONSHIP_COUNT; i++ )
            {
                assertEquals( 1, count( node.getRelationships( type( i ) ) ),
                        "Should be possible to get relationships of type with id in unsigned short range." );
            }
        }
    }

    private static void shiftHighId( GraphDatabaseAPI db )
    {
        RecordStorageEngine storageEngine = db.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        NeoStores neoStores = storageEngine.testAccessNeoStores();
        neoStores.getRelationshipTypeTokenStore().setHighId( Short.MAX_VALUE - RELATIONSHIP_COUNT / 2 );
    }

    private static RelationshipType type( int i )
    {
        return RelationshipType.withName( "TYPE_" + i );
    }
}
