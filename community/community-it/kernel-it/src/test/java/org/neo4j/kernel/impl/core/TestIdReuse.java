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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class TestIdReuse
{
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void makeSureIdsGetsReusedForPropertyStore()
    {
        makeSureIdsGetsReused( testDirectory.databaseLayout().propertyStore(), 10, 200 );
    }

    @Test
    void makeSureIdsGetsReusedForArrayStore()
    {
        long[] array = new long[500];
        for ( int i = 0; i < array.length; i++ )
        {
            array[i] = 0xFFFFFFFFFFFFL + i;
        }
        makeSureIdsGetsReused( testDirectory.databaseLayout().propertyArrayStore(), array, 20 );
    }

    @Test
    void makeSureIdsGetsReusedForStringStore()
    {
        String string = "something";
        for ( int i = 0; i < 100; i++ )
        {
            string += "something else " + i;
        }
        makeSureIdsGetsReused( testDirectory.databaseLayout().propertyStringStore(), string, 20 );
    }

    private void makeSureIdsGetsReused( File storeFile, Object value, int iterations )
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( fileSystem )
                .impermanent()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        for ( int i = 0; i < 5; i++ )
        {
            setAndRemoveSomeProperties( db, value );
        }
        managementService.shutdown();
        long sizeBefore = storeFile.length();
        DatabaseManagementService impermanentManagement = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( fileSystem )
                .impermanent()
                .build();
        db = impermanentManagement.database( DEFAULT_DATABASE_NAME );
        for ( int i = 0; i < iterations; i++ )
        {
            setAndRemoveSomeProperties( db, value );
        }
        managementService.shutdown();
        assertEquals( sizeBefore, storeFile.length() );
    }

    private void setAndRemoveSomeProperties( GraphDatabaseService graphDatabaseService, Object value )
    {
        Node commonNode;
        try ( Transaction transaction = graphDatabaseService.beginTx() )
        {
            commonNode = graphDatabaseService.createNode();
            for ( int i = 0; i < 10; i++ )
            {
                commonNode.setProperty( "key" + i, value );
            }
            transaction.success();
        }

        try ( Transaction transaction = graphDatabaseService.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                commonNode.removeProperty( "key" + i );
            }
            transaction.success();
        }
    }
}
