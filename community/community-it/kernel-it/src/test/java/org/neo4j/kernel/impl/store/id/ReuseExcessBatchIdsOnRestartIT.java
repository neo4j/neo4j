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
package org.neo4j.kernel.impl.store.id;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;

public class ReuseExcessBatchIdsOnRestartIT
{
    @Rule
    public final DatabaseRule db = new EmbeddedDatabaseRule();

    // Knowing that ids are grabbed in batches internally we only create one node and later assert
    // that the excess ids that were only grabbed, but not used can be reused.
    @Test
    public void shouldReuseExcessBatchIdsWhichWerentUsedBeforeClose() throws Exception
    {
        // given
        Node firstNode;
        try ( Transaction tx = db.beginTx() )
        {
            firstNode = db.createNode();
            tx.success();
        }

        // when
        db.restartDatabase();

        Node secondNode;
        try ( Transaction tx = db.beginTx() )
        {
            secondNode = db.createNode();
            tx.success();
        }

        // then
        assertEquals( firstNode.getId() + 1, secondNode.getId() );
    }
}
