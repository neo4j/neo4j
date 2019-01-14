/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api;

import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.kernel.api.query.ExecutingQuery;

abstract class ExecutingQueryList
{
    abstract Stream<ExecutingQuery> queries();

    abstract ExecutingQueryList push( ExecutingQuery newExecutingQuery );

    final ExecutingQueryList remove( ExecutingQuery executingQuery )
    {
        return remove( null, executingQuery );
    }

    abstract ExecutingQueryList remove( ExecutingQuery parent, ExecutingQuery target );

    abstract <T> T top( Function<ExecutingQuery,T> accessor );

    abstract void waitsFor( ExecutingQuery query );

    static final ExecutingQueryList EMPTY = new ExecutingQueryList()
    {
        @Override
        Stream<ExecutingQuery> queries()
        {
            return Stream.empty();
        }

        @Override
        ExecutingQueryList push( ExecutingQuery newExecutingQuery )
        {
            return new Entry( newExecutingQuery, this );
        }

        @Override
        ExecutingQueryList remove( ExecutingQuery parent, ExecutingQuery target )
        {
            return this;
        }

        @Override
        <T> T top( Function<ExecutingQuery,T> accessor )
        {
            return null;
        }

        @Override
        void waitsFor( ExecutingQuery query )
        {
        }
    };

    private static class Entry extends ExecutingQueryList
    {
        private final ExecutingQuery query;
        private final ExecutingQueryList next;

        Entry( ExecutingQuery query, ExecutingQueryList next )
        {
            this.query = query;
            this.next = next;
        }

        @Override
        Stream<ExecutingQuery> queries()
        {
            Stream.Builder<ExecutingQuery> builder = Stream.builder();
            ExecutingQueryList entry = this;
            while ( entry != EMPTY )
            {
                Entry current = (Entry) entry;
                builder.accept( current.query );
                entry = current.next;
            }
            return builder.build();
        }

        @Override
        ExecutingQueryList push( ExecutingQuery newExecutingQuery )
        {
            assert newExecutingQuery.internalQueryId() > query.internalQueryId();
            waitsFor( newExecutingQuery );
            return new Entry( newExecutingQuery, this );
        }

        @Override
        ExecutingQueryList remove( ExecutingQuery parent, ExecutingQuery target )
        {
            if ( target.equals( query ) )
            {
                next.waitsFor( parent );
                return next;
            }
            else
            {
                ExecutingQueryList removed = next.remove( parent, target );
                if ( removed == next )
                {
                    return this;
                }
                else
                {
                    return new Entry( query, removed );
                }
            }
        }

        @Override
        <T> T top( Function<ExecutingQuery,T> accessor )
        {
            return accessor.apply( query );
        }

        @Override
        void waitsFor( ExecutingQuery child )
        {
            this.query.waitsForQuery( child );
        }
    }
}
