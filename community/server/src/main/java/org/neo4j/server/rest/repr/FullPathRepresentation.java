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
package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.Path;

public class FullPathRepresentation extends ObjectRepresentation
{
    private final Path path;

    public FullPathRepresentation( Path path )
    {
        super( RepresentationType.FULL_PATH );
        this.path = path;
    }

    @Mapping( "start" )
    public NodeRepresentation startNode()
    {
        return new NodeRepresentation( path.startNode() );
    }

    @Mapping( "end" )
    public NodeRepresentation endNode()
    {
        return new NodeRepresentation( path.endNode() );
    }

    @Mapping( "length" )
    public ValueRepresentation length()
    {
        return ValueRepresentation.number( path.length() );
    }

    @Mapping( "nodes" )
    public ListRepresentation nodes()
    {
        return NodeRepresentation.list( path.nodes() );
    }

    @Mapping( "relationships" )
    public ListRepresentation relationships()
    {
        return RelationshipRepresentation.list( path.relationships() );
    }
}
