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

import org.neo4j.graphdb.QueryStatistics;

public class CypherStatisticsRepresentation extends MappingRepresentation
{
    private final QueryStatistics stats;

    public CypherStatisticsRepresentation( QueryStatistics stats )
    {
        super( "stats" );
        this.stats = stats;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putBoolean( "contains_updates", stats.containsUpdates() );
        serializer.putNumber( "nodes_created", stats.getNodesCreated() );
        serializer.putNumber( "nodes_deleted", stats.getNodesDeleted() );
        serializer.putNumber( "properties_set", stats.getPropertiesSet() );
        serializer.putNumber( "relationships_created", stats.getRelationshipsCreated() );
        serializer.putNumber( "relationship_deleted", stats.getRelationshipsDeleted() );
        serializer.putNumber( "labels_added", stats.getLabelsAdded() );
        serializer.putNumber( "labels_removed", stats.getLabelsRemoved() );
        serializer.putNumber( "indexes_added", stats.getIndexesAdded() );
        serializer.putNumber( "indexes_removed", stats.getIndexesRemoved() );
        serializer.putNumber( "constraints_added", stats.getConstraintsAdded() );
        serializer.putNumber( "constraints_removed", stats.getConstraintsRemoved() );
    }
}
