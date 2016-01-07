/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.proc;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;

public interface Procedure
{
    ProcedureSignature signature();
    Stream<Object[]> apply( Context ctx, Object[] input ) throws ProcedureException;

    /**
     * The context in which a procedure is invoked. This is a read-only map-like structure. For instance, a read-only transactional procedure might have
     * access to the current statement it is being invoked in through this.
     *
     * The context is entirely defined by the caller of the procedure, so what is available in the context depends on the context of the call.
     */
    interface Context
    {
        <T> T get( Key<T> key ) throws ProcedureException;
    }

    /**
     * We use this little wrapper to get some basic type checking and help us remember which type of object we should get out for a given key.
     * @param <T>
     */
    interface Key<T>
    {
        String name();

        static <T> Key<T> key( String name, Class<T> type )
        {
            return () -> name;
        }
    }

    /**
     * Not thread safe. Basic context backed by a map.
     */
    class BasicContext implements Context
    {
        private final Map<String, Object> values = new HashMap<>();

        @Override
        public <T> T get( Key<T> key ) throws ProcedureException
        {
            Object o = values.get( key.name() );
            if( o == null ) {
                throw new ProcedureException( Status.Procedure.CallFailed, "There is no `%s` in the current procedure call context.", key.name() );
            }
            return (T) o;
        }

        public <T> void put( Key<T> key, T value )
        {
            values.put( key.name(), value );
        }
    }

    abstract class BasicProcedure implements Procedure
    {
        private final ProcedureSignature signature;

        protected BasicProcedure( ProcedureSignature signature )
        {
            this.signature = signature;
        }

        @Override
        public ProcedureSignature signature()
        {
            return signature;
        }

        @Override
        public abstract Stream<Object[]> apply( Context ctx, Object[] input ) throws ProcedureException;
    }
}
