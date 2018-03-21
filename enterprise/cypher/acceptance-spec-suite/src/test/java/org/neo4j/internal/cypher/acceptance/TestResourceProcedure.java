/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.proc.ComponentRegistry;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

public class TestResourceProcedure
{
    abstract static class SimulateFailureBaseException extends RuntimeException
    {
    }

    public static class SimulateFailureException extends SimulateFailureBaseException
    {
    }

    public static class SimulateFailureOnCloseException extends SimulateFailureBaseException
    {
    }

    @Context
    public GraphDatabaseService db;

    @Context
    public Counters counters;

    public static class Counters
    {
        public int closeCountTestResourceProcedure;
        public int closeCountTestFailingResourceProcedure;
        public int closeCountTestOnCloseFailingResourceProcedure;

        public int openCountTestResourceProcedure;
        public int openCountTestFailingResourceProcedure;
        public int openCountTestOnCloseFailingResourceProcedure;

        public int liveCountTestResourceProcedure()
        {
            return openCountTestResourceProcedure - closeCountTestResourceProcedure;
        }

        public int liveCountTestFailingResourceProcedure()
        {
            return openCountTestFailingResourceProcedure - closeCountTestFailingResourceProcedure;
        }

        public int liveCountTestOnCloseFailingResourceProcedure()
        {
            return openCountTestOnCloseFailingResourceProcedure - closeCountTestOnCloseFailingResourceProcedure;
        }

        public void reset()
        {
            closeCountTestResourceProcedure = 0;
            closeCountTestFailingResourceProcedure = 0;
            closeCountTestOnCloseFailingResourceProcedure = 0;
            openCountTestResourceProcedure = 0;
            openCountTestFailingResourceProcedure = 0;
            openCountTestOnCloseFailingResourceProcedure = 0;
        }
    }

    public static ComponentRegistry.Provider<Counters> countersProvider( Counters counters )
    {
        return context -> counters;
    }

    public class Output
    {
        public Long value;

        public Output( Long value )
        {
            this.value = value;
        }
    }

    @Procedure( name = "org.neo4j.test.testResourceProcedure", mode = Mode.READ )
    @Description( "Returns a stream of integers from 1 to the given argument" )
    public Stream<Output> testResourceProcedure( @Name( value = "resultCount", defaultValue = "4" ) long resultCount ) throws Exception
    {
        Stream<Output> stream = Stream.iterate( 1L, i -> i + 1 ).limit( resultCount ).map( Output::new );
        stream.onClose( () -> {
            counters.closeCountTestResourceProcedure++;
        } );
        counters.openCountTestResourceProcedure++;
        return stream;
    }

    @Procedure( name = "org.neo4j.test.testFailingResourceProcedure", mode = Mode.READ )
    @Description( "Returns a stream of integers from 1 to the given argument, but throws an exception when reaching the last element" )
    public Stream<Output> testFailingResourceProcedure( @Name( value = "failCount", defaultValue = "3" ) long failCount ) throws Exception
    {
        Iterator<Output> failingIterator = new Iterator<Output>()
        {
            private long step = 1;

            @Override
            public boolean hasNext()
            {
                return step <= failCount;
            }

            @Override
            public Output next()
            {
                if ( step == failCount )
                {
                    throw new SimulateFailureException();
                }
                return new Output( step++ );
            }
        };
        Iterable<Output> failingIterable = () -> failingIterator;
        Stream<Output> stream = StreamSupport.stream( failingIterable.spliterator(), false );
        stream.onClose( () -> {
            counters.closeCountTestFailingResourceProcedure++;
        } );
        counters.openCountTestFailingResourceProcedure++;
        return stream;
    }

    @Procedure( name = "org.neo4j.test.testOnCloseFailingResourceProcedure", mode = Mode.READ )
    @Description( "Returns a stream of integers from 1 to the given argument. Throws an exception on close." )
    public Stream<Output> testOnCloseFailingResourceProcedure( @Name( value = "resultCount", defaultValue = "4" ) long resultCount ) throws Exception
    {
        Stream<Output> stream = Stream.iterate( 1L, i -> i + 1 ).limit( resultCount ).map( Output::new );
        stream.onClose( () -> {
            counters.closeCountTestOnCloseFailingResourceProcedure++;
            throw new SimulateFailureOnCloseException();
        } );
        counters.openCountTestOnCloseFailingResourceProcedure++;
        return stream;
    }

    @UserFunction( name = "org.neo4j.test.fail" )
    @Description( "org.neo4j.test.fail" )
    public String fail( @Name( value = "input" ) String input )
    {
        throw new SimulateFailureException();
    }
}
