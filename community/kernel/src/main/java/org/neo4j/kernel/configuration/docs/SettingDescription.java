/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.configuration.docs;

import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a configuration item to be included into the generated Asciidoc.
 */
public final class SettingDescription
{
    private final String id;
    private final String name;
    private final String description;
    private final String mandatoryDescription;
    private final String deprecationDescription;
    private final String validationDescription;
    private final String defaultValue;
    private final boolean isDeprecated;
    private final boolean isMandatory;
    private final boolean hasDefault;

    public SettingDescription( String id, String name, String description, String mandatoryDescription, String
            deprecationDescription,
            String validationDescription, String defaultValue,
            boolean isDeprecated, boolean isMandatory, boolean hasDefault )
    {
        this.id = id;
        this.mandatoryDescription = mandatoryDescription;
        this.deprecationDescription = deprecationDescription;
        this.validationDescription = validationDescription;
        this.defaultValue = defaultValue;
        this.isDeprecated = isDeprecated;
        this.name = name.replace( "{", "\\{" ).replace( "}", "\\}" );
        this.description = description;
        this.isMandatory = isMandatory;
        this.hasDefault = hasDefault;
    }

    public SettingDescription( String id, String name, String description )
    {
        this( id, name, description, null, null, null, null, false, false, false );
    }

    public String id()
    {
        return id;
    }

    public String name()
    {
        return name;
    }

    public String description()
    {
        return description;
    }

    public boolean isDeprecated()
    {
        return isDeprecated;
    }

    public boolean hasDefault()
    {
        //if ( !defaultValue.equals( DEFAULT_MARKER ) )
        return hasDefault;
    }

    public String defaultValue()
    {
        return defaultValue;
    }

    public boolean isMandatory()
    {
        return isMandatory;
    }

    public String mandatoryDescription()
    {
        // Note MANDATORY
        return mandatoryDescription;
    }

    public String deprecationMessage()
    {
        // Note OBSOLETED & DEPRECATED
        return deprecationDescription;
    }

    public String validationMessage()
    {
        // Note VALIDATION_MESSAGE
        return validationDescription;
    }

    /**
     * Return a new item with all prose descriptions formatted using
     * the passed-in format.
     */
    public SettingDescription formatted( Function<String, String> format )
    {
        Function<String,String> f = ( str ) -> str == null ? null : format.apply(str);
        return new SettingDescription(
                id, name,
                f.apply( description ),
                f.apply(mandatoryDescription),
                f.apply(deprecationDescription),

                // I don't like this, but validationdescription contains a lot of
                // technical terms, and the formatters barf on it. Leave it out for now,
                // which is what the old impl did, and improve the formatters at some point
                validationDescription,
                defaultValue,
                isDeprecated, isMandatory, hasDefault );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SettingDescription that = (SettingDescription) o;
        return Objects.equals( name, that.name ) &&
               Objects.equals( description, that.description );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name, description );
    }

    @Override
    public String toString()
    {
        return "SettingDescription{" + "id='" + id() + "\', name='" + name + "\', description='" + description + "\'}";
    }
}
