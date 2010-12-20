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

package org.neo4j.server.plugins;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ExtensionPointRepresentation;
import org.neo4j.server.rest.repr.Representation;

public abstract class ExtensionPoint
{
    private final String name;
    private final Class<?> extendsType;
    private final String description;

    protected ExtensionPoint( Class<?> type, String name, String description )
    {
        this.extendsType = type;
        this.description = description == null ? "" : description;
        this.name = ServerPlugin.verifyName( name );
    }

    protected ExtensionPoint( Class<?> type, String name )
    {
        this( type, name, null );
    }

    public final String name()
    {
        return name;
    }

    public final Class<?> forType()
    {
        return extendsType;
    }

    public abstract Representation invoke( AbstractGraphDatabase graphDb, Object context,
            ParameterList params ) throws BadInputException, BadExtensionInvocationException,
            PluginInvocationFailureException;

    final ExtensionPointRepresentation descibe()
    {
        ExtensionPointRepresentation representation = new ExtensionPointRepresentation( name,
                extendsType, description );
        describeParameters( representation );
        return representation;
    }

    protected void describeParameters( ParameterDescriptionConsumer consumer )
    {
    }
}
