/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;

public class BaseWorker
{
    protected final Index<Node> index;
    protected final GraphDatabaseService graphDb;
    private volatile Exception exception;
    private final ExecutorService commandExecutor = newSingleThreadExecutor();
    private final CommandState state;

    public BaseWorker( Index<Node> index, GraphDatabaseService graphDb )
    {
        this.index = index;
        this.graphDb = graphDb;
        this.state = new CommandState( index, graphDb );
    }

    protected void queueCommand( final Command cmd )
    {
        try
        {
            commandExecutor.submit( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        cmd.doWork( state );
                    }
                    catch ( Exception e )
                    {
                        exception = e;
                    }
                }
            } ).get();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public boolean hasException()
    {
        return exception != null;
    }
}
