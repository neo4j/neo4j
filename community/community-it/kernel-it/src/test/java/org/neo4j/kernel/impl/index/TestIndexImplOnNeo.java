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
package org.neo4j.kernel.impl.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.index.IndexManager.PROVIDER;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class TestIndexImplOnNeo
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;

    @BeforeEach
    void createDb()
    {
        db = new TestGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .newImpermanentDatabase( testDirectory.databaseDir() );
    }

    private void restartDb()
    {
        shutdownDb();
        createDb();
    }

    @AfterEach
    void shutdownDb()
    {
        db.shutdown();
    }

    @Test
    void createIndexWithProviderThatUsesNeoAsDataSource()
    {
        String indexName = "inneo";
        assertFalse( indexExists( indexName ) );
        Map<String, String> config = stringMap( PROVIDER, "test-dummy-neo-index",
                "config1", "A value", "another config", "Another value" );

        Index<Node> index;
        try ( Transaction transaction = db.beginTx() )
        {
            index = db.index().forNodes( indexName, config );
            transaction.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertTrue( indexExists( indexName ) );
            assertEquals( config, db.index().getConfiguration( index ) );
            try ( IndexHits<Node> indexHits = index.get( "key", "something else" ) )
            {
                assertEquals( 0, Iterables.count( indexHits ) );
            }
            tx.success();
        }

        restartDb();

        try ( Transaction tx = db.beginTx() )
        {
            assertTrue( indexExists( indexName ) );
            assertEquals( config, db.index().getConfiguration( index ) );
            tx.success();
        }
    }

    private boolean indexExists( String indexName )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            boolean exists = db.index().existsForNodes( indexName );
            transaction.success();
            return exists;
        }
    }
}
