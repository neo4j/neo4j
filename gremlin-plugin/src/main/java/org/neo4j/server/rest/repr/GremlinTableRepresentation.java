/**
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
package org.neo4j.server.rest.repr;

import java.util.List;

import com.tinkerpop.pipes.util.structures.Row;
import com.tinkerpop.pipes.util.structures.Table;
import org.neo4j.helpers.collection.IterableWrapper;

public class GremlinTableRepresentation extends ObjectRepresentation
{

    private final Table queryResult;

    public GremlinTableRepresentation( Table result )
    {
        super( RepresentationType.STRING );
        this.queryResult = result;
    }

    @Mapping("columns")
    public Representation columns()
    {
        return ListRepresentation.string( queryResult.getColumnNames() );
    }

    @Mapping("data")
    public Representation data()
    {
        final List<String> columnNames = queryResult.getColumnNames();
        final IterableWrapper<Representation, Row> rows = new IterableWrapper<Representation, Row>( queryResult )
        {
            protected Representation underlyingObjectToObject( Row row )
            {
                return new ListRepresentation( "row", convertRow( row, columnNames ) );
            }
        };
        return new ListRepresentation( "data", rows );
    }

    private Iterable<Representation> convertRow( final Row row, final List<String> columnNames )
    {
        return new IterableWrapper<Representation, String>( columnNames )
        {
            protected Representation underlyingObjectToObject( String column )
            {
                final Object fieldValue = row.getColumn( column );
                return GremlinObjectToRepresentationConverter.convert( fieldValue );
            }
        };
    }
}
