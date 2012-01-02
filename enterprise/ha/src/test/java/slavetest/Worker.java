/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package slavetest;

import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.OtherThreadExecutor;

public class Worker extends OtherThreadExecutor<WorkerState>
{
    public Worker( GraphDatabaseService db )
    {
        super( new WorkerState( db ) );
    }
    
    public void beginTx() throws Exception
    {
        execute( new WorkerCommand<WorkerState, Void>()
        {
            @Override
            public Void doWork( WorkerState state )
            {
                state.beginTx();
                return null;
            }
        } );
    }
    
    public void finishTx( final boolean success ) throws Exception
    {
        execute( new WorkerCommand<WorkerState, Void>()
        {
            @Override
            public Void doWork( WorkerState state )
            {
                state.finishTx( success );
                return null;
            }
        } );
    }
    
    public Future<Boolean> putIfAbsent( final String index, final long node, final String key, final Object value ) throws Exception
    {
        return executeDontWait( new WorkerCommand<WorkerState, Boolean>()
        {
            @Override
            public Boolean doWork( WorkerState state )
            {
                return state.db.index().forNodes( index ).putIfAbsent( state.db.getNodeById( node ), key, value );
            }
        } );
    }
}
