/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.toList;

public class UniquenessConstraintHaIT extends ConstraintHaIT
{
    @Override
    protected void createConstraint( GraphDatabaseService db, Label label, String propertyKey )
    {
        db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
    }

    @Override
    protected void createConstraintOffendingNode( GraphDatabaseService db, Label label, String propertyKey,
            String propertyValue )
    {
        db.createNode( label ).setProperty( propertyKey, propertyValue );
    }

    @Override
    protected void assertConstraintHolds( GraphDatabaseService db, Label label, String propertyKey,
            String propertyValue )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, toList( db.findNodes( label, propertyKey, propertyValue ) ).size() );
            tx.success();
        }
    }
}
