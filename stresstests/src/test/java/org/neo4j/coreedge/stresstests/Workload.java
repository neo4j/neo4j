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
package org.neo4j.coreedge.stresstests;

import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Node;

class Workload extends RepeatUntilCallable
{
    private Cluster cluster;

    Workload( BooleanSupplier keepGoing, Runnable onFailure, Cluster cluster )
    {
        super( keepGoing, onFailure );
        this.cluster = cluster;
    }

    @Override
    protected void doWork()
    {
        try
        {
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode();
                node.setProperty( "prop1", "let's add some data here so the transaction logs rotate more often..." );
                node.setProperty( "prop2", "let's add some data here so the transaction logs rotate more often..." );
                node.setProperty( "prop3", "let's add some data here so the transaction logs rotate more often..." );
                node.setProperty( "prop4", "let's add some data here so the transaction logs rotate more often..." );
                node.setProperty( "prop5", "let's add some data here so the transaction logs rotate more often..." );
                node.setProperty( "prop6", "let's add some data here so the transaction logs rotate more often..." );
                node.setProperty( "prop7", "let's add some data here so the transaction logs rotate more often..." );
                node.setProperty( "prop8", "let's add some data here so the transaction logs rotate more often..." );
                tx.success();
            } );
        }
        catch ( InterruptedException e )
        {
            // whatever let's go on with the workload
            Thread.interrupted();
        }
        catch ( TimeoutException | DatabaseShutdownException e )
        {
            // whatever let's go on with the workload
        }
    }
}
