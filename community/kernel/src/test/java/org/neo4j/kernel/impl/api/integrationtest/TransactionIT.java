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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import javax.transaction.TransactionManager;

public class TransactionIT extends KernelIntegrationTest
{
    @Test
    public void shouldBeAbleToCommitThroughTransactionManager() throws Exception
    {
        // Given
        TransactionManager txManager = db.getDependencyResolver().resolveDependency( TransactionManager.class );

        db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Bob" );

        // When
        txManager.commit();

        // Then write locks should have been released, so I can write to the node from a new tx
        try( Transaction tx = db.beginTx() )
        {
            node.setProperty( "name", "Other" );
            tx.success();
        }
    }
}
