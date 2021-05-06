/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.capabilities;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nonnull;

import static java.lang.String.format;

/**
 * Used for hierarchical naming of capabilities, each namespace component separated by '.'.
 */
public final class Name
{
    private static final String SEPARATOR = ".";

    private final String fullName;

    private Name( @Nonnull String fullName )
    {
        this.fullName = validateName( Objects.requireNonNull( fullName ) );
    }

    /**
     * Returns the full name.
     *
     * @return full name.
     */
    @Nonnull
    public String fullName()
    {
        return fullName;
    }

    /**
     * Creates a child name from this name instance.
     *
     * @param name child name.
     * @return new name instance.
     * @throws IllegalArgumentException if name is empty or contains '.'.
     */
    @Nonnull
    public Name child( @Nonnull String name )
    {
        if ( StringUtils.isBlank( name ) || StringUtils.contains( name, SEPARATOR ) )
        {
            throw new IllegalArgumentException( String.format( "'%s' is not a valid name", name ) );
        }

        if ( StringUtils.isBlank( this.fullName ) )
        {
            return new Name( name );
        }

        return new Name( this.fullName + SEPARATOR + Objects.requireNonNull( name ) );
    }

    /**
     * Checks if this name instance is in the given namespace.
     *
     * @param namespace namespace to check
     * @return true if this name lies in the given namespace, false otherwise.
     */
    public boolean isIn( @Nonnull String namespace )
    {
        var validated = validateName( namespace );

        if ( StringUtils.isBlank( validated ) || validated.equals( fullName ) )
        {
            return true;
        }

        return fullName.startsWith( validated + SEPARATOR );
    }

    /**
     * Checks if this name instance is in the given name's scope.
     *
     * @param name name to check
     * @return true if this name lies in the given name's scope, false otherwise.
     */
    public boolean isIn( @Nonnull Name name )
    {
        return isIn( name.fullName );
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

        Name other = (Name) o;
        return this.fullName.equals( other.fullName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( fullName );
    }

    @Override
    public String toString()
    {
        return fullName;
    }

    @Nonnull
    private static String validateName( @Nonnull String fullName )
    {
        var comps = StringUtils.split( fullName, SEPARATOR );
        var invalid = Arrays.stream( comps ).anyMatch( StringUtils::isWhitespace );

        if ( invalid )
        {
            throw new IllegalArgumentException( format( "'%s' is not a valid name.", fullName ) );
        }

        return fullName;
    }

    /**
     * Creates a name from the given string.
     *
     * @param name name.
     * @return a new name instance.
     */
    @Nonnull
    public static Name of( @Nonnull String name )
    {
        return new Name( name );
    }
}
