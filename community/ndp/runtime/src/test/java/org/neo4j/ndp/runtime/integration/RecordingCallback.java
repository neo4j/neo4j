/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.runtime.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.StatementMetadata;
import org.neo4j.ndp.runtime.internal.Neo4jError;
import org.neo4j.stream.Record;
import org.neo4j.stream.RecordStream;

public class RecordingCallback implements Session.Callback
{
    private final BlockingQueue<Call> calls = new ArrayBlockingQueue<>( 64 );

    public Call next() throws InterruptedException
    {
        Call msg = calls.poll( 10, TimeUnit.SECONDS );
        if(msg == null)
        {
            throw new RuntimeException( "Waited 10 seconds for message, but no message arrived." );
        }
        return msg;
    }

    private List<Call> results = new LinkedList<>();
    private List<Neo4jError> errors = new LinkedList<>();
    private boolean ignored;

    @Override
    public void result( Object result, Object attachment )
    {
        if ( result instanceof RecordStream )
        {
            RecordStream curs = (RecordStream) result;
            try
            {
                results.add( new Result( unwind( curs ) ) );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
        else if(result instanceof StatementMetadata)
        {
            results.add( new StatementSuccess( (StatementMetadata) result ) );
        }
        else
        {
            throw new RuntimeException( "Unknown result type: " + result );
        }
    }

    @Override
    public void failure( Neo4jError err, Object attachment )
    {
        errors.add( err );
    }

    @Override
    public void completed( Object attachment )
    {
        try
        {
            if(ignored)
            {
                calls.add(new Ignored());
            }
            else if ( errors.size() > 0 )
            {
                calls.add( new Failure( errors.get( 0 ) ) );
            }
            else if ( results.size() > 1 )
            {
                throw new IllegalStateException( "Cannot handle multiple results yet." );
            }
            else if ( results.size() == 1 )
            {
                calls.add( results.get( 0 ) );
            }
            else
            {
                calls.add( new Success() );
            }
        } finally
        {
            results.clear();
            errors.clear();
            ignored = false;
        }
    }

    @Override
    public void ignored( Object attachment )
    {
        this.ignored = true;
    }

    public static class Call
    {
        public boolean isSuccess()
        {
            return false;
        }

        public Neo4jError error()
        {
            throw new IllegalStateException( "Expected a failure, but this call did not fail. Got: " + getClass()
                    .getSimpleName() );
        }

        public boolean isIgnored()
        {
            return false;
        }
    }

    public static class Success extends Call
    {
        @Override
        public boolean isSuccess()
        {
            return true;
        }
    }

    public static class Result extends Success
    {
        private final Object[] streamValues;

        public Result( Object[] streamValues )
        {
            this.streamValues = streamValues;
        }

        public Object[] records()
        {
            return streamValues;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder( "Table{" );
            sb.append( "records=[" ).append( Arrays.toString( streamValues ) ).append( "]" );
            return sb.append( "}" ).toString();
        }
    }

    public static class StatementSuccess extends Success
    {
        private final StatementMetadata meta;

        public StatementSuccess(StatementMetadata meta)
        {
            this.meta = meta;
        }

        public StatementMetadata meta()
        {
            return meta;
        }
    }

    public static class Ignored extends Call
    {
        @Override
        public boolean isIgnored()
        {
            return true;
        }
    }

    public static class Failure extends Call
    {
        private final Neo4jError err;

        public Failure( Neo4jError err )
        {
            this.err = err;
        }

        @Override
        public Neo4jError error()
        {
            return err;
        }

        @Override
        public String toString()
        {
            return err.toString();
        }
    }

    private Object[] unwind( RecordStream result ) throws Exception
    {
        final List<Object> values = new ArrayList<>();
        result.visitAll( new RecordStream.Visitor()
        {
            @Override
            public void visit( Record record )
            {
                values.add( record.copy() );
            }
        } );
        return values.toArray();
    }
}
