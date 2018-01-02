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
package org.neo4j.kernel.impl.api.store;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.test.ImpermanentDatabaseRule;

/**
 * This test ensures that lazy properties
 */
public class TestReferenceDangling
{
    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule( );

    @Test
    public void testPropertyStoreReferencesOnRead() throws Throwable
    {
        // Given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // and Given the cache contains a LazyProperty
        long nId = ensurePropertyIsCachedLazyProperty( db, "some" );

        // When
        restartNeoDataSource( db );

        // Then reading the property is still possible
        try( Transaction tx = db.beginTx() )
        {
            db.getNodeById( nId ).getProperty( "some" );
            tx.success();
        }
    }

    @Test
    public void testPropertyStoreReferencesOnWrite() throws Throwable
    {
        // Given
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        // and Given the cache contains a LazyProperty
        long nId = ensurePropertyIsCachedLazyProperty( db, "some" );

        // When
        restartNeoDataSource( db );

        // Then it should still be possible to manipulate properties on this node
        try( Transaction tx = db.beginTx() )
        {
            db.getNodeById( nId ).setProperty( "some", new long[]{-1,2,2,3,4,5,5} );
            tx.success();
        }
    }

    private long ensurePropertyIsCachedLazyProperty( GraphDatabaseAPI slave, String key )
    {
        long nId;
        try( Transaction tx = slave.beginTx() )
        {
            Node n = slave.createNode();
            nId = n.getId();
            n.setProperty( key, new long[]{-1,2,2,3,4,5,5} );
            tx.success();
        }

        try( Transaction tx = slave.beginTx() )
        {
            slave.getNodeById( nId ).hasProperty( key );
            tx.success();
        }
        return nId;
    }

    private void restartNeoDataSource( GraphDatabaseAPI slave ) throws Throwable
    {
        slave.getDependencyResolver().resolveDependency( DataSourceManager.class ).getDataSource().stop();
        slave.getDependencyResolver().resolveDependency( DataSourceManager.class ).getDataSource().start();
    }
}
