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
package org.neo4j.server.rest.repr;

import java.util.function.Function;

import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import static org.neo4j.helpers.collection.Iterables.map;

public class IndexDefinitionRepresentation extends MappingRepresentation
{
    private final IndexDefinition indexDefinition;
    private final IndexPopulationProgress indexPopulationProgress;
    private final Schema.IndexState indexState;

    public IndexDefinitionRepresentation( IndexDefinition indexDefinition )
    {
        // Online state will mean progress is ignored
        this( indexDefinition, Schema.IndexState.ONLINE, IndexPopulationProgress.DONE );
    }

    public IndexDefinitionRepresentation( IndexDefinition indexDefinition, Schema.IndexState indexState,
            IndexPopulationProgress indexPopulationProgress )
    {
        super( RepresentationType.INDEX_DEFINITION );
        this.indexDefinition = indexDefinition;
        this.indexPopulationProgress = indexPopulationProgress;
        this.indexState = indexState;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putString( "label", indexDefinition.getLabel().name() );
        Function<String,Representation> converter = ValueRepresentation::string;
        Iterable<Representation> propertyKeyRepresentations = map( converter, indexDefinition.getPropertyKeys() );
        serializer.putList( "property_keys", new ListRepresentation( RepresentationType.STRING,
                propertyKeyRepresentations ) );
        // Only print state and progress if progress is a valid value and not yet online
        if ( indexState == Schema.IndexState.POPULATING )
        {
            serializer.putString( "state", indexState.name() );
            serializer.putString( "population_progress", String.format( "%1.0f%%",
                    indexPopulationProgress.getCompletedPercentage() ) );
        }
    }
}
