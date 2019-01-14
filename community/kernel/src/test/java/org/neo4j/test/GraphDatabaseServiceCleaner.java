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
package org.neo4j.test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

public class GraphDatabaseServiceCleaner
{
    private GraphDatabaseServiceCleaner()
    {
        throw new UnsupportedOperationException();
    }

    public static void cleanDatabaseContent( GraphDatabaseService db )
    {
        cleanupSchema( db );
        cleanupAllRelationshipsAndNodes( db );
    }

    public static void cleanupSchema( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                constraint.drop();
            }

            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                index.drop();
            }
            tx.success();
        }
    }

    public static void cleanupAllRelationshipsAndNodes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Relationship relationship : db.getAllRelationships() )
            {
                relationship.delete();
            }

            for ( Node node : db.getAllNodes() )
            {
                node.delete();
            }
            tx.success();
        }
    }
}
