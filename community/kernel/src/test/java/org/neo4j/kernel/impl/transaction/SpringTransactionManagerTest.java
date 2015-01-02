/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import javax.transaction.TransactionManager;

import static org.junit.Assert.assertEquals;

public class SpringTransactionManagerTest
{
    @Test(timeout = 5000)
    public void testDoubleUpdateWithJavaTM() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabase();
        TransactionManager tm = new SpringTransactionManager( db );
        tm.begin();

        Node node;
        node = db.createNode();
        node.setProperty( "name", "Foo" );
        tm.commit();

        Transaction transaction = db.beginTx();
        assertEquals( "Foo", db.getNodeById( node.getId() ).getProperty( "name" ) );
        node.setProperty( "name", "Bar" );
        transaction.success();
        transaction.finish();

        tm.begin();
        node.setProperty( "name", "FooBar" );
        assertEquals( "FooBar", db.getNodeById( node.getId() ).getProperty(
                "name" ) );
        tm.commit();
    }
}
