/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.management;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestLockManagerBean
{
    private LockManager lockManager;

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
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

        try ( Transaction ignore = graphDb.beginTx() )
        {
            node.setProperty( "key", "value" );

            List<LockInfo> locks = lockManager.getLocks();
            assertEquals( "unexpected lock count", 1, locks.size() );
            LockInfo lock = locks.get( 0 );
            assertNotNull( "null lock", lock );

        }
        List<LockInfo> locks = lockManager.getLocks();
        assertEquals( "unexpected lock count", 0, locks.size() );
    }

    private Node createNode()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            tx.success();
            return node;
        }
    }

}
