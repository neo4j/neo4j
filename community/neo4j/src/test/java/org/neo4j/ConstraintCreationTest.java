/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

public class ConstraintCreationTest
{
    @Rule
    public EmbeddedDatabaseRule database = new EmbeddedDatabaseRule();

    @Test
    public void createConstraint()
    {
        Label label = Label.label( "testLabel" );
        String property = "property";
        int counter = 0;
        for ( int i = 0; i < 100; i++ )
        {
            try ( Transaction tx = database.beginTx() )
            {
                Node node = database.createNode( label );
                node.setProperty( property, counter++ );
                tx.success();
            }
        }

        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            transaction.success();
        }

        database.shutdownAndKeepStore();
        System.out.println("Find database in : " + database.getStoreDir());
    }
}
