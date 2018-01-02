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

import org.neo4j.kernel.internal.Version;

import static org.neo4j.server.rest.web.Surface.PATH_BATCH;
import static org.neo4j.server.rest.web.Surface.PATH_CYPHER;
import static org.neo4j.server.rest.web.Surface.PATH_EXTENSIONS;
import static org.neo4j.server.rest.web.Surface.PATH_LABELS;
import static org.neo4j.server.rest.web.Surface.PATH_NODES;
import static org.neo4j.server.rest.web.Surface.PATH_NODE_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIPS;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIP_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_RELATIONSHIP_TYPES;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_CONSTRAINT;
import static org.neo4j.server.rest.web.Surface.PATH_SCHEMA_INDEX;
import static org.neo4j.server.rest.web.Surface.PATH_TRANSACTION;

public class DatabaseRepresentation extends MappingRepresentation implements ExtensibleRepresentation
{

    public DatabaseRepresentation()
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
        serializer.putRelativeUri( "node", PATH_NODES );
        serializer.putRelativeUri( "relationship", PATH_RELATIONSHIPS );
        serializer.putRelativeUri( "node_index", PATH_NODE_INDEX );
        serializer.putRelativeUri( "relationship_index", PATH_RELATIONSHIP_INDEX );
        serializer.putRelativeUri( "extensions_info", PATH_EXTENSIONS );
        serializer.putRelativeUri( "relationship_types", PATH_RELATIONSHIP_TYPES );
        serializer.putRelativeUri( "batch", PATH_BATCH );
        serializer.putRelativeUri( "cypher", PATH_CYPHER );
        serializer.putRelativeUri( "indexes", PATH_SCHEMA_INDEX );
        serializer.putRelativeUri( "constraints", PATH_SCHEMA_CONSTRAINT );
        serializer.putRelativeUri( "transaction", PATH_TRANSACTION );
        serializer.putRelativeUri( "node_labels", PATH_LABELS );
        serializer.putString( "neo4j_version", Version.getNeo4jVersion() );
    }
}
