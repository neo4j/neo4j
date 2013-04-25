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
package org.neo4j.kernel.impl.transaction;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

@Ignore( "Not a test, merely a debugging helper or something" )
public class ResourceIteratorInTxIT
{
    @Test
    public void shouldNotStrainCleanupServiceWhenLeavingStuffBehindWhenInsideTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        Label label = DynamicLabel.label( "PERSON" );
        {
            Transaction tx = db.beginTx();
            try
            {
                db.schema().indexCreator( label ).on( "name" ).create();
                for ( int i = 0; i < 10; i++ )
                {
                    Node node = db.createNode( label );
                    node.setProperty( "name", "Mattias" );
                }
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }

        // WHEN
        while ( true )
        {
            Transaction tx = db.tx().unforced().begin();
            try
            {
                ResourceIterator<Node> iterator =
                        db.findNodesByLabelAndProperty( label, "name", "Mattias" ).iterator();
                iterator.next(); // Don't exhaust it
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    }
}
