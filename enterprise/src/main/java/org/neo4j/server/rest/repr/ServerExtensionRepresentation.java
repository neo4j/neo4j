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

import java.util.List;

public final class ServerExtensionRepresentation extends MappingRepresentation
{
    private final List<ExtensionPointRepresentation> methods;

    public ServerExtensionRepresentation( String name, List<ExtensionPointRepresentation> methods )
    {
        super( RepresentationType.SERVER_EXTENSION_DESCRIPTION );
        this.methods = methods;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putList( "extension-points", new ListRepresentation(
                RepresentationType.EXTENSION_POINT_DESCRIPTION, methods ) );
    }
}
