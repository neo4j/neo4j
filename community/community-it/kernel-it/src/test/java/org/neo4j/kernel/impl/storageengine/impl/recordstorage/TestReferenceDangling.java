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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

/**
 * This test ensures that lazy properties
 */
@ImpermanentDbmsExtension
class TestReferenceDangling
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void testPropertyStoreReferencesOnRead() throws Throwable
    {
        // Given the cache contains a LazyProperty
        long nId = ensurePropertyIsCachedLazyProperty( db, "some" );

        // When
        restartNeoDataSource( db );

        // Then reading the property is still possible
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nId ).getProperty( "some" );
            tx.commit();
        }
    }

    @Test
    void testPropertyStoreReferencesOnWrite() throws Throwable
    {
        // Given the cache contains a LazyProperty
        long nId = ensurePropertyIsCachedLazyProperty( db, "some" );

        // When
        restartNeoDataSource( db );

        // Then it should still be possible to manipulate properties on this node
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nId ).setProperty( "some", new long[]{-1, 2, 2, 3, 4, 5, 5} );
            tx.commit();
        }
    }

    private long ensurePropertyIsCachedLazyProperty( GraphDatabaseService slave, String key )
    {
        long nId;
        try ( Transaction tx = slave.beginTx() )
        {
            Node n = tx.createNode();
            nId = n.getId();
            n.setProperty( key, new long[]{-1, 2, 2, 3, 4, 5, 5} );
            tx.commit();
        }

        try ( Transaction tx = slave.beginTx() )
        {
            tx.getNodeById( nId ).hasProperty( key );
            tx.commit();
        }
        return nId;
    }

    private void restartNeoDataSource( GraphDatabaseAPI databaseAPI ) throws Throwable
    {
        Database database = databaseAPI.getDependencyResolver().resolveDependency( Database.class );
        database.stop();
        database.start();
    }
}
