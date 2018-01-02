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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.server.plugins.ParameterDescriptionConsumer;

public final class ExtensionPointRepresentation extends ObjectRepresentation implements ParameterDescriptionConsumer
{
    private final RepresentationType extended;
    private final String name;
    private final String desciption;
    private final List<ParameterRepresentation> parameters = new ArrayList<ParameterRepresentation>();

    public ExtensionPointRepresentation( String name, Class<?> extended, String desciption )
    {
        super( RepresentationType.PLUGIN_DESCRIPTION );
        this.name = name;
        this.desciption = desciption;
        this.extended = RepresentationType.extended( extended );
    }

    @Override
    public void describeParameter( String name, Class<?> type, boolean optional, String description )
    {
        parameters.add( new ParameterRepresentation( name, type, optional, description, false ) );
    }

    @Override
    public void describeListParameter( String name, Class<?> type, boolean optional, String description )
    {
        parameters.add( new ParameterRepresentation( name, type, optional, description, true ) );
    }

    public String getName()
    {
        return name;
    }

    public String getExtendedEntity()
    {
        return extended.valueName;
    }

    @Mapping( "name" )
    public ValueRepresentation methodName()
    {
        return ValueRepresentation.string( name );
    }

    @Mapping( "description" )
    public ValueRepresentation description()
    {
        return ValueRepresentation.string( desciption );
    }

    @Mapping( "extends" )
    public ValueRepresentation extendedEntity()
    {
        return ValueRepresentation.string( getExtendedEntity() );
    }

    @Mapping( "parameters" )
    public ListRepresentation parametersList()
    {
        return new ListRepresentation( RepresentationType.PLUGIN_PARAMETER, parameters );
    }

    private static class ParameterRepresentation extends MappingRepresentation
    {
        private final String name;
        private final RepresentationType paramType;
        private final String description;
        private final boolean optional;
        private final boolean list;

        ParameterRepresentation( String name, Class<?> type, boolean optional, String description, boolean list )
        {
            super( RepresentationType.PLUGIN_PARAMETER );
            this.name = name;
            this.optional = optional;
            this.list = list;
            this.paramType = RepresentationType.extended( type );
            this.description = description;
        }

        @Override
        protected void serialize( MappingSerializer serializer )
        {
            serializer.putString( "name", name );
            serializer.putString( "type", list ? paramType.listName : paramType.valueName );
            serializer.putBoolean( "optional", optional );
            serializer.putString( "description", description );
        }
    }
}
