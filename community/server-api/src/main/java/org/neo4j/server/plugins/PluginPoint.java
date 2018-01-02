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
package org.neo4j.server.plugins;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.Representation;

public abstract class PluginPoint
{
    private final String name;
    private final Class<?> extendsType;
    private final String description;

    protected PluginPoint( Class<?> type, String name, String description )
    {
        this.extendsType = type;
        this.description = description == null ? "" : description;
        this.name = ServerPlugin.verifyName( name );
    }

    protected PluginPoint( Class<?> type, String name )
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

    public String getDescription()
    {
        return description;
    }

    public abstract Representation invoke( GraphDatabaseAPI graphDb, Object context,
            ParameterList params ) throws BadInputException, BadPluginInvocationException,
            PluginInvocationFailureException;


    protected void describeParameters( ParameterDescriptionConsumer consumer )
    {
    }
}
