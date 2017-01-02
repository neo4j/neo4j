/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.configuration;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * A configuration option with its active value.
 */
public class ConfigValue
{
    private final String name;
    private final Optional<String> description;
    private final Optional<?> value;

    public ConfigValue( @Nonnull String name, @Nonnull Optional<String> description, @Nonnull Optional<?> value )
    {
        this.name = name;
        this.description = description;
        this.value = value;
    }

    @Nonnull
    public String name()
    {
        return name;
    }

    @Nonnull
    public Optional<String> description()
    {
        return description;
    }

    @Nonnull
    public Optional<?> value()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return value.map( Object::toString ).orElse( "null" );
    }
}
