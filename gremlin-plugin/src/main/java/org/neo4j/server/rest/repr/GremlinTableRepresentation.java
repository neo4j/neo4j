/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.server.plugin.gremlin.GremlinToRepresentationConverter;

import com.tinkerpop.pipes.util.Table;
import com.tinkerpop.pipes.util.Table.Row;

public class GremlinTableRepresentation extends ObjectRepresentation
{

    private final Table queryResult;
    private final GremlinToRepresentationConverter converter = new GremlinToRepresentationConverter();

    public GremlinTableRepresentation(Table result)
    {
        super( RepresentationType.STRING );
        this.queryResult = result;
    }

    @Mapping( "columns" )
    public Representation columns()
    {
        return ListRepresentation.string( queryResult.getColumnNames() );
    }

    @Mapping( "data" )
    public Representation data()
    {
        List<Representation> rows = new ArrayList<Representation>();
        for (  Row  row : queryResult )
        {
            rows.add( new ListRepresentation( "row", convertRow(row)) );
        }
        return new ListRepresentation( "data", rows );
    }

    private List<Representation> convertRow(Row row) {
        List<Representation> fields = new ArrayList<Representation>();
        for ( String column : queryResult.getColumnNames() )
        {
            final Object fieldValue = row.getColumn(column);
            Representation fieldRepresentation = converter.convert(fieldValue);
            fields.add( fieldRepresentation );

        }
        return fields;
    }
}
