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

import org.neo4j.function.Function;
import org.neo4j.graphdb.schema.IndexDefinition;

import static org.neo4j.helpers.collection.Iterables.map;

public class IndexDefinitionRepresentation extends MappingRepresentation
{
    private final IndexDefinition indexDefinition;

    public IndexDefinitionRepresentation( IndexDefinition indexDefinition )
    {
        super( RepresentationType.INDEX_DEFINITION );
        this.indexDefinition = indexDefinition;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putString( "label", indexDefinition.getLabel().name() );
        Function<String, Representation> converter = new Function<String, Representation>()
        {
            @Override
            public Representation apply( String propertyKey )
            {
                return ValueRepresentation.string( propertyKey );
            }
        };
        Iterable<Representation> propertyKeyRepresentations = map( converter, indexDefinition.getPropertyKeys() );
        serializer.putList( "property_keys", new ListRepresentation( RepresentationType.STRING,
                propertyKeyRepresentations ) );
    }
}
