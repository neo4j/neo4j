/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.*;

public class TestLockManagerBean
{
    private LockManager lockManager;

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    @SuppressWarnings("deprecation")
    private GraphDatabaseAPI graphDb;

    @Before
    public void setup()
    {
        graphDb = dbRule.getGraphDatabaseAPI();
        lockManager = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class )
                .getSingleManagementBean( LockManager.class );
    }

    @Test
    public void restingGraphHoldsNoLocks()
    {
        assertEquals( "unexpected lock count", 0, lockManager.getLocks().size() );
    }

    @Test
    public void modifiedNodeImpliesLock()
    {
        Node node = createNode();

        try(Transaction ignore = graphDb.beginTx())
        {
            node.setProperty( "key", "value" );

            List<LockInfo> locks = lockManager.getLocks();
            assertEquals( "unexpected lock count", 2, locks.size() );
            LockInfo lock = locks.get( 0 );
            assertNotNull( "null lock", lock );

        }
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 0, locks.size() );
    }

    private Node createNode()
    {
        try( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            tx.success();
            return node;
        }
    }

    private LockInfo getSingleLock()
    {
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 1, locks.size() );
        LockInfo lock = locks.get( 0 );
        assertNotNull( "null lock", lock );
        return lock;
    }
}
