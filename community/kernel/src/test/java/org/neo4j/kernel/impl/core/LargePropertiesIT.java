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
package org.neo4j.kernel.impl.core;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class LargePropertiesIT
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void readArrayAndStringPropertiesWithDifferentBlockSizes()
    {
        String stringValue = RandomStringUtils.randomAlphanumeric( 10000 );
        byte[] arrayValue = RandomStringUtils.randomAlphanumeric( 10000 ).getBytes();

        GraphDatabaseService db = new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() )
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.string_block_size, "1024" )
                .setConfig( GraphDatabaseSettings.array_block_size, "2048" )
                .newGraphDatabase();
        try
        {
            long nodeId;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                nodeId = node.getId();
                node.setProperty( "string", stringValue );
                node.setProperty( "array", arrayValue );
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.getNodeById( nodeId );
                assertEquals( stringValue, node.getProperty( "string" ) );
                assertArrayEquals( arrayValue, (byte[]) node.getProperty( "array" ) );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }
}
