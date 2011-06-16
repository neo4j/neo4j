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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.plugin.gremlin.GremlinPlugin;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.gremlin.pipes.util.Table;
import com.tinkerpop.gremlin.pipes.util.Table.Row;

public class GremlinTableRepresentation extends ObjectRepresentation
{

    private final Table queryResult;
    private final Neo4jGraph graph;

    public GremlinTableRepresentation( Table result, Neo4jGraph graph )
    {
        super( RepresentationType.STRING );
        this.queryResult = result;
        this.graph = graph;
    }

    @Mapping( "columns" )
    public Representation columns()
    {

        return ListRepresentation.string( queryResult.getColumnNames() );
    }

    @Mapping( "data" )
    public Representation data()
    {
//        // rows
        List<Representation> rows = new ArrayList<Representation>();
        for (  Row  row : queryResult )
        {
            List<Representation> fields = new ArrayList<Representation>();
            // columns
            for ( String column : queryResult.getColumnNames() )
            {
                Representation rowRep = GremlinPlugin.getRepresentation( graph, row.getColumn( column ) );
                fields.add( rowRep );

            }
            rows.add( new ListRepresentation( "row", fields ) );
        }
        return new ListRepresentation( "data", rows );
    }

    

}
