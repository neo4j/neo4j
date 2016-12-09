/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.backup.stresstests;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helper.RepeatUntilCallable;

class WorkLoad extends RepeatUntilCallable
{
    private static final Label label = Label.label( "Label" );
    private final Supplier<GraphDatabaseService> dbRef;

    WorkLoad( BooleanSupplier keepGoing, Runnable onFailure, Supplier<GraphDatabaseService> dbRef )
    {
        super( keepGoing, onFailure );
        this.dbRef = dbRef;
    }

    @Override
    protected void doWork()
    {
        GraphDatabaseService db = dbRef.get();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            for ( int i = 1; i <= 8; i++ )
            {
                node.setProperty( prop( i ), "let's add some data here so the transaction logs rotate more often..." );
            }
            tx.success();
        }
        catch ( DatabaseShutdownException | TransactionFailureException e )
        {
            // whatever let's go on with the workload
        }
    }

    static void setupIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 1; i <= 8; i++ )
            {
                db.schema().indexFor( label ).on( prop( i ) ).create();
            }
            tx.success();
        }
    }

    private static String prop( int i )
    {
        return "prop" + i;
    }
}
