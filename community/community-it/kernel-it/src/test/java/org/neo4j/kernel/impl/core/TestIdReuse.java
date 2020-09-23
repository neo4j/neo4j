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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@EphemeralNeo4jLayoutExtension
class TestIdReuse
{
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    @Disabled
    void makeSureIdsGetsReusedForPropertyStore() throws IOException
    {
        makeSureIdsGetsReused( databaseLayout.propertyStore(), 10, 200 );
    }

    @Test
    @Disabled
    void makeSureIdsGetsReusedForArrayStore() throws IOException
    {
        long[] array = new long[500];
        for ( int i = 0; i < array.length; i++ )
        {
            array[i] = 0xFFFFFFFFFFFFL + i;
        }
        makeSureIdsGetsReused( databaseLayout.propertyArrayStore(), array, 20 );
    }

    @Test
    @Disabled
    void makeSureIdsGetsReusedForStringStore() throws IOException
    {
        String string = "something";
        for ( int i = 0; i < 100; i++ )
        {
            string += "something else " + i;
        }
        makeSureIdsGetsReused( databaseLayout.propertyStringStore(), string, 20 );
    }

    private void makeSureIdsGetsReused( Path storeFile, Object value, int iterations ) throws IOException
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .impermanent()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        for ( int i = 0; i < 5; i++ )
        {
            setAndRemoveSomeProperties( db, value );
        }
        managementService.shutdown();
        long sizeBefore = fileSystem.getFileSize( storeFile );
        assertTrue( sizeBefore > 0 );
        DatabaseManagementService impermanentManagement = new TestDatabaseManagementServiceBuilder( databaseLayout )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fileSystem ) )
                .impermanent()
                .build();
        db = impermanentManagement.database( DEFAULT_DATABASE_NAME );
        for ( int i = 0; i < iterations; i++ )
        {
            setAndRemoveSomeProperties( db, value );
        }
        impermanentManagement.shutdown();
        assertEquals( sizeBefore, fileSystem.getFileSize( storeFile ) );
    }

    private void setAndRemoveSomeProperties( GraphDatabaseService graphDatabaseService, Object value )
    {
        Node commonNode;
        try ( Transaction transaction = graphDatabaseService.beginTx() )
        {
            commonNode = transaction.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                commonNode.setProperty( "key" + i, value );
            }
            transaction.commit();
        }

        try ( Transaction transaction = graphDatabaseService.beginTx() )
        {
            var txNode = transaction.getNodeById( commonNode.getId() );
            for ( int i = 0; i < 10; i++ )
            {
                txNode.removeProperty( "key" + i );
            }
            transaction.commit();
        }
    }
}
