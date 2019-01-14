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
package org.neo4j.bolt.v1.runtime.spi;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.values.AnyValue;

/**
 * Represents a data stream of records, this is the output cypher produces.
 *
 * Streams contains nominally uniform records meaning each record has the same set of named fields.
 * However, the contents of these fields may vary by both type and value and may be null.
 */
public abstract class BoltResult implements AutoCloseable
{
    /** Positional names for all fields in every record of this stream. */
    public abstract String[] fieldNames();

    public abstract void accept( Visitor visitor ) throws Exception;

    @Override
    public abstract void close();

    public interface Visitor
    {
        void visit( QueryResult.Record record ) throws Exception;

        /**
         * Associate arbitrary metadata with the result stream. This will get transferred at the end of the stream.
         * Please stick to Neo4j type system types (Map, List, Integer, Float, Boolean, String etc) */
        void addMetadata( String key, AnyValue value );
    }

    public static final BoltResult EMPTY = new BoltResult()
    {
        private final String[] nothing = new String[0];

        @Override
        public void close()
        {

        }

        @Override
        public String[] fieldNames()
        {
            return nothing;
        }

        @Override
        public void accept( Visitor visitor )
        {

        }
    };
}
