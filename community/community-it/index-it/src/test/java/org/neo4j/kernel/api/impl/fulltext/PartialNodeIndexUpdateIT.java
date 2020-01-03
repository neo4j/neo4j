/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertNotNull;

public class PartialNodeIndexUpdateIT
{
    @Rule
    public DatabaseRule db = new EmbeddedDatabaseRule();

    @Test
    public void partialIndexedNodePropertiesUpdate()
    {
        GraphDatabaseService database = db.getGraphDatabaseAPI();
        final Label userLabel = Label.label( "User" );
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "CALL db.index.fulltext.createNodeIndex('test', ['Card', '" + userLabel.name() + "'], " +
                            "['title', 'plainText', 'username', 'screenName'] )" );
            transaction.success();
        }

        final String value = "asdf";
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "UNWIND [{_id:48, properties:{screenName:\"" + value + "\"}}] as row " +
                    "CREATE (n:L1{_id: row._id}) SET n += row.properties SET n:" + userLabel.name() );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            assertNotNull( database.findNode( userLabel, "screenName", value ) );
        }
    }
}
