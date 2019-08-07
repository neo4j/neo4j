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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( EphemeralFileSystemExtension.class )
class LargePropertiesIT
{
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Test
    void readArrayAndStringPropertiesWithDifferentBlockSizes()
    {
        String stringValue = RandomStringUtils.randomAlphanumeric( 10000 );
        byte[] arrayValue = RandomStringUtils.randomAlphanumeric( 10000 ).getBytes();

        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder().setFileSystem( fs ).impermanent()
                .setConfig( GraphDatabaseSettings.string_block_size, 1024 )
                .setConfig( GraphDatabaseSettings.array_block_size, 2048 ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            long nodeId;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                nodeId = node.getId();
                node.setProperty( "string", stringValue );
                node.setProperty( "array", arrayValue );
                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.getNodeById( nodeId );
                assertEquals( stringValue, node.getProperty( "string" ) );
                assertArrayEquals( arrayValue, (byte[]) node.getProperty( "array" ) );
                tx.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
