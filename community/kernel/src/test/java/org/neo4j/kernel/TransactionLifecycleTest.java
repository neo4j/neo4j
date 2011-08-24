/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TargetDirectory;


public class TransactionLifecycleTest
{
    public TargetDirectory target = TargetDirectory.forTest( getClass() );
    private EmbeddedGraphDatabase graphdb;
    
    @Before
    public void startGraphdb()
    {
        this.graphdb = new EmbeddedGraphDatabase( target.graphDbDir( true ).getPath() );
    }

    @After
    public void stopGraphdb()
    {
        if ( graphdb != null ) graphdb.shutdown();
        graphdb = null;
    }
    
    @Test(expected=NotFoundException.class)
    public void givenACallToFailATransactionSubsequentSuccessCallsShouldBeSwallowedSilently() {
        Transaction tx = graphdb.beginTx();
        Node someNode = null;
        try {
            someNode = graphdb.createNode();
            tx.failure();
            
            tx.success();
        } finally {
            tx.finish();
        }
        
        // Belt and braces
        assertNull(graphdb.getNodeById( someNode.getId() ));
    }
}
