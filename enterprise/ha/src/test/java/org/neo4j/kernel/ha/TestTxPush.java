/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.test.TargetDirectory.forTest;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TargetDirectory;

@Ignore("Need to properly setup instances with priorities such that slave only instances are present")
public class TestTxPush
{
    private final TargetDirectory dir = forTest( getClass() );

    @Test
    public void testMasterCapableIsAheadOfSlaveOnlyRegardlessOfPriority() throws Exception
    {
        HighlyAvailableGraphDatabase master = null,
                slave1 = null,
                slave2 = null;
        try
        {
            /*
             * Need to initialize a master with push factor 1, one slave only and one master capable, with the
             * slave only higher in priority than the other.
             */
            Transaction tx = master.beginTx();
            Node node = master.createNode();
            long nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
            tx.finish();

            try
            {
                slave1.getNodeById( nodeId );
                fail("It shouldn't be in the slave only instance");
            }
            catch ( NotFoundException e )
            {
                // not there, as supposed to
            }

            assertEquals( "bar", slave2.getNodeById( nodeId ).getProperty( "foo" ) );
        }
        finally
        {
            if ( slave2 != null )
            {
                slave2.shutdown();
            }
            if ( slave1 != null )
            {
                slave1.shutdown();
            }
            if ( master != null )
            {
                master.shutdown();
            }
        }
    }

    @Test
    public void testSlaveOnlyWillNotGetPushedAtToMeetQuota() throws Exception
    {
        HighlyAvailableGraphDatabase master = null,
                slave1 = null,
                slave2 = null;
        try
        {
            /*
             * Instance initialization missing - need to have one master with push factor 2, one master capable and
             * one slave only.
             */

            Transaction tx = master.beginTx();
            Node node = master.createNode();
            long nodeId = node.getId();
            node.setProperty( "foo", "bar" );
            tx.success();
            tx.finish();

            /*
             * This is the slave only, it will not get transactions pushed at even though it is in higher prio and
             * the push factor is 2
             */

            try
            {
                slave1.getNodeById( nodeId );
                fail("It shouldn't be in the slave only instance");
            }
            catch ( NotFoundException e )
            {
                // fine
            }

            assertEquals( "bar", slave2.getNodeById( nodeId ).getProperty( "foo" ) );
        }
        finally
        {
            if ( slave2 != null )
            {
                slave2.shutdown();
            }
            if ( slave1 != null )
            {
                slave1.shutdown();
            }
            if ( master != null )
            {
                master.shutdown();
            }
        }
    }
}
