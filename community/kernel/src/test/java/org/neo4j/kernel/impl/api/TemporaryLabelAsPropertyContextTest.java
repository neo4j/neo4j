/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.test.ImpermanentGraphDatabase;

public class TemporaryLabelAsPropertyContextTest
{
    @Test
    public void should_be_able_to_add_label_to_node() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        long nodeId = db.createNode().getId();
        String labelName = "mylabel";
        long labelId = statement.getOrCreateLabelId( labelName );

        // WHEN
        statement.addLabelToNode( labelId, nodeId );
        tx.success();
        tx.finish();

        // THEN
        assertTrue( "Label " + labelName + " wasn't set on " + nodeId, statement.isLabelSetOnNode( labelId, nodeId ) );
    }

    //write test when we dont have a property map

    private GraphDatabaseAPI db;
    private StatementContext statement;

    @Before
    public void before()
    {
        db = new ImpermanentGraphDatabase();
        statement = new TemporaryLabelAsPropertyStatementContext(
                db.getDependencyResolver().resolveDependency( PropertyIndexManager.class ),
                db.getDependencyResolver().resolveDependency( PersistenceManager.class ) );
    }

    @After
    public void after()
    {
        db.shutdown();
    }
}
