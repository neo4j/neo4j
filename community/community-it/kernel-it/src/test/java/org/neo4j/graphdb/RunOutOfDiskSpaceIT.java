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
package org.neo4j.graphdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.limited.LimitedFilesystemAbstraction;

import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Neo4jLayoutExtension
class RunOutOfDiskSpaceIT
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    FileSystemAbstraction fileSystem;
    @Inject
    DatabaseLayout databaseLayout;
    private LimitedFilesystemAbstraction limitedFs;
    private GraphDatabaseAPI database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        limitedFs = new LimitedFilesystemAbstraction( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) );
        managementService = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( limitedFs )
                .build();
        database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    @Test
    void shouldPropagateIOExceptions() throws Exception
    {
        try ( Transaction tx = database.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }

        long logVersion = database.getDependencyResolver().resolveDependency( LogVersionRepository.class )
                            .getCurrentLogVersion();

        limitedFs.runOutOfDiskSpace( true );

        // When
        TransactionFailureException exception = assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.createNode();
                tx.commit();
            }
        } );
        assertTrue( indexOfThrowable( exception, IOException.class ) != -1 );

        limitedFs.runOutOfDiskSpace( false ); // to help shutting down the db
        managementService.shutdown();

        PageCache pageCache = pageCacheExtension.getPageCache( limitedFs );
        File neoStore = databaseLayout.metadataStore();
        assertEquals( logVersion, MetaDataStore.getRecord( pageCache, neoStore, MetaDataStore.Position.LOG_VERSION ) );
    }

    @Test
    void shouldStopDatabaseWhenOutOfDiskSpace() throws Exception
    {
        try ( Transaction tx = database.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }

        long logVersion = database.getDependencyResolver().resolveDependency( LogVersionRepository.class )
                .getCurrentLogVersion();

        limitedFs.runOutOfDiskSpace( true );

        assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.createNode();
                tx.commit();
            }
        } );

        // When
        assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction ignored = database.beginTx() )
            {
                fail( "Expected tx begin to throw TransactionFailureException when tx manager breaks." );
            }
        } );

        // Then
        limitedFs.runOutOfDiskSpace( false ); // to help shutting down the database
        managementService.shutdown();

        PageCache pageCache = pageCacheExtension.getPageCache( limitedFs );
        File neoStore = databaseLayout.metadataStore();
        assertEquals( logVersion, MetaDataStore.getRecord( pageCache, neoStore, MetaDataStore.Position.LOG_VERSION ) );
    }

}
