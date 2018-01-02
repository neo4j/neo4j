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
package org.neo4j.cypher.internal.tracing;

import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer;

public interface CompilationTracer
{
    QueryCompilationEvent compileQuery( String query );

    interface QueryCompilationEvent extends AutoCloseable, CompilationPhaseTracer
    {
        @Override
        void close();
    }

    CompilationTracer NO_COMPILATION_TRACING = new CompilationTracer()
    {
        @Override
        public QueryCompilationEvent compileQuery( String query )
        {
            return NONE_EVENT;
        }

        private final QueryCompilationEvent NONE_EVENT = new QueryCompilationEvent()
        {
            @Override
            public CompilationPhaseEvent beginPhase( CompilationPhase phase )
            {
                return NONE_PHASE;
            }

            @Override
            public void close()
            {
            }
        };
    };
}
