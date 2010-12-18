/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.GraphDatabaseService;

public class DatabaseRepresentation extends MappingRepresentation implements
        ExtensibleRepresentation
{
    private final GraphDatabaseService graphDb;

    public DatabaseRepresentation( GraphDatabaseService graphDb )
    {
        super( RepresentationType.GRAPHDB );
        this.graphDb = graphDb;
    }

    @Override
    public String getIdentity()
    {
        // This is in fact correct - there is only one graphdb - hence no id
        return null;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( "node", "node" );
        serializer.putRelativeUri( "reference_node",
                NodeRepresentation.path( graphDb.getReferenceNode() ) );
        serializer.putRelativeUri( "node-index", "index/node" );
        serializer.putRelativeUri( "relationship-index", "index/relationship" );
        serializer.putRelativeUri( "extensions-info", "ext" );
    }
}
