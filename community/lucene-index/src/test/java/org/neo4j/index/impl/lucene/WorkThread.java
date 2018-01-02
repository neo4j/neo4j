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
package org.neo4j.index.impl.lucene;

import java.util.Map;
import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.test.OtherThreadExecutor;

public class WorkThread extends OtherThreadExecutor<CommandState>
{
    private volatile boolean txOngoing;

    public WorkThread( String name, Index<Node> index, GraphDatabaseService graphDb, Node node )
    {
        super( name, new CommandState( index, graphDb, node ) );
    }

    public void createNodeAndIndexBy( String key, String value ) throws Exception
    {
        execute( new CreateNodeAndIndexByCommand( key, value ) );
    }

    public void deleteIndex() throws Exception
    {
        execute( new DeleteIndexCommand() );
    }

    public IndexHits<Node> queryIndex( String key, Object value ) throws Exception
    {
        return execute( new QueryIndexCommand( key, value ) );
    }

    public void commit() throws Exception
    {
        execute( new CommitCommand() );
        txOngoing = false;
    }

    public void beginTransaction() throws Exception
    {
        assert !txOngoing;
        execute( new BeginTransactionCommand() );
        txOngoing = true;
    }

    public void removeFromIndex( String key, String value ) throws Exception
    {
        execute( new RemoveFromIndexCommand( key, value ) );
    }

    public void rollback() throws Exception
    {
        if ( !txOngoing ) return;
        execute( new RollbackCommand() );
        txOngoing = false;
    }

    public void die() throws Exception
    {
        execute( new DieCommand() );
    }

    public Future<Node> putIfAbsent( Node node, String key, Object value ) throws Exception
    {
        return executeDontWait( new PutIfAbsentCommand( node, key, value ) );
    }

    public void add( final Node node, final String key, final Object value ) throws Exception
    {
        execute( new WorkerCommand<CommandState, Void>()
        {
            @Override
            public Void doWork( CommandState state )
            {
                state.index.add( node, key, value );
                return null;
            }
        } );
    }

    public Future<Node> getOrCreate( final String key, final Object value, final Object initialValue ) throws Exception
    {
        return executeDontWait( new WorkerCommand<CommandState, Node>()
        {
            @Override
            public Node doWork( CommandState state )
            {
                UniqueFactory.UniqueNodeFactory factory = new UniqueFactory.UniqueNodeFactory( state.index )
                {
                    @Override
                    protected void initialize( Node node, Map<String, Object> properties )
                    {
                        node.setProperty( key, initialValue );
                    }
                };
                return factory.getOrCreate( key, value );
            }
        } );
    }

    public Object getProperty( final PropertyContainer entity, final String key ) throws Exception
    {
        return execute( new WorkerCommand<CommandState, Object>()
        {
            @Override
            public Object doWork( CommandState state )
            {
                return entity.getProperty( key );
            }
        } );
    }
}
