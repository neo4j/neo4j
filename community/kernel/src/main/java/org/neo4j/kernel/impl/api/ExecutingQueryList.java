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
package org.neo4j.kernel.impl.api;

import java.util.stream.Stream;

import org.neo4j.kernel.api.ExecutingQuery;

interface ExecutingQueryList
{
    Stream<ExecutingQuery> queries();

    ExecutingQueryList push( ExecutingQuery newExecutingQuery );

    ExecutingQueryList remove( ExecutingQuery executingQuery );

    ExecutingQueryList EMPTY = new Empty();

    class Entry implements ExecutingQueryList
    {
        final ExecutingQuery query;
        final ExecutingQueryList next;

        Entry( ExecutingQuery query, ExecutingQueryList next )
        {
            this.query = query;
            this.next = next;
        }

        @Override
        public Stream<ExecutingQuery> queries()
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
        public ExecutingQueryList push( ExecutingQuery newExecutingQuery )
        {
            assert( newExecutingQuery.internalQueryId() > query.internalQueryId() );
            return new Entry( newExecutingQuery, this );
        }

        @Override
        public ExecutingQueryList remove( ExecutingQuery executingQuery )
        {
            if ( executingQuery.equals( query ) )
            {
                return next;
            }
            else
            {
                return next == EMPTY ? EMPTY : new Entry( query, next.remove( executingQuery ) );
            }
        }
    }

    class Empty implements ExecutingQueryList
    {
        @Override
        public Stream<ExecutingQuery> queries()
        {
            return Stream.empty();
        }

        @Override
        public ExecutingQueryList push( ExecutingQuery newExecutingQuery )
        {
            return new Entry(newExecutingQuery, this);
        }

        @Override
        public ExecutingQueryList remove( ExecutingQuery executingQuery )
        {
            return this;
        }
    }
}
