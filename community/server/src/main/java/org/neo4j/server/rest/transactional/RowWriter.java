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
package org.neo4j.server.rest.transactional;

import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;

import org.neo4j.graphdb.Result;

class RowWriter implements ResultDataContentWriter
{
    @Override
    public void write( JsonGenerator out, Iterable<String> columns, Result.ResultRow row,
            TransactionStateChecker txStateChecker ) throws IOException
    {
        out.writeArrayFieldStart( "row" );
        try
        {
            for ( String key : columns )
            {
                out.writeObject( row.get( key ) );
            }
        }
        finally
        {
            out.writeEndArray();
            writeMeta( out, columns, row );
        }
    }

    private void writeMeta( JsonGenerator out, Iterable<String> columns, Result.ResultRow row ) throws IOException
    {
        out.writeArrayFieldStart( "meta" );
        try
        {
            /*
             * The way we've designed this JSON serialization is by injecting a custom codec
             * to write the entities. Unfortunately, there seems to be no way to control state
             * inside the JsonGenerator, and so we need to make a second call to write out the
             * meta information, directly to the injected codec. This is not very pretty,
             * but time is expensive, and redesigning one of three server serialization
             * formats is not a priority.
             */
            Neo4jJsonCodec codec = (Neo4jJsonCodec) out.getCodec();
            for ( String key : columns )
            {
                codec.writeMeta( out, row.get( key ) );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }
}
