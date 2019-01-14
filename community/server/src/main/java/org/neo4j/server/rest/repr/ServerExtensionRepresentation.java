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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ServerExtensionRepresentation extends MappingRepresentation
{
    private final Map<String, EntityExtensionRepresentation> extended;

    public ServerExtensionRepresentation( String name, List<ExtensionPointRepresentation> methods )
    {
        super( RepresentationType.SERVER_PLUGIN_DESCRIPTION );
        this.extended = new HashMap<>();
        for ( ExtensionPointRepresentation extension : methods )
        {
            EntityExtensionRepresentation entity =
                    extended.computeIfAbsent( extension.getExtendedEntity(), k -> new EntityExtensionRepresentation() );
            entity.add( extension );
        }
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        for ( Map.Entry<String, EntityExtensionRepresentation> entity : extended.entrySet() )
        {
            serializer.putMapping( entity.getKey(), entity.getValue() );
        }
    }

    private static class EntityExtensionRepresentation extends MappingRepresentation
    {
        private final List<ExtensionPointRepresentation> extensions;

        EntityExtensionRepresentation()
        {
            super( "entity-extensions" );
            this.extensions = new ArrayList<>();
        }

        void add( ExtensionPointRepresentation extension )
        {
            extensions.add( extension );
        }

        @Override
        protected void serialize( MappingSerializer serializer )
        {
            for ( ExtensionPointRepresentation extension : extensions )
            {
                serializer.putMapping( extension.getName(), extension );
            }
        }
    }
}
