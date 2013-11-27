/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.Version;

public class DatabaseRepresentation extends MappingRepresentation implements ExtensibleRepresentation
{
    public DatabaseRepresentation( GraphDatabaseService graphDb )
    {
        super( RepresentationType.GRAPHDB );
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
        serializer.putUri( "node", "node" );
        serializer.putUri( "node_index", "index/node" );
        serializer.putUri( "relationship_index", "index/relationship" );
        serializer.putUri( "extensions_info", "ext" );
        serializer.putUri( "relationship_types", "relationship/types" );
        serializer.putUri( "batch", "batch" );
        serializer.putUri( "cypher", "cypher" );
        serializer.putUri( "transaction", "transaction" );
        serializer.putString( "neo4j_version", Version.getKernel().getReleaseVersion() );
    }
}
