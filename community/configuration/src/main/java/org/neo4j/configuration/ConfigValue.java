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
package org.neo4j.configuration;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * A configuration option with its active value.
 */
public class ConfigValue
{
    private final String name;
    private final Optional<String> description;
    private final Optional<String> documentedDefaultValue;
    private final Optional<Object> value;
    private final String valueDescription;
    private final boolean internal;
    private final boolean secret;
    private final boolean dynamic;
    private final boolean deprecated;
    private final Optional<String> replacement;

    public ConfigValue( @Nonnull String name, @Nonnull Optional<String> description,
            @Nonnull Optional<String> documentedDefaultValue, @Nonnull Optional<Object> value,
            @Nonnull String valueDescription, boolean internal, boolean dynamic, boolean deprecated,
            @Nonnull Optional<String> replacement, boolean secret )
    {
        this.name = name;
        this.description = description;
        this.documentedDefaultValue = documentedDefaultValue;
        this.value = value;
        this.valueDescription = valueDescription;
        this.internal = internal;
        this.secret = secret;
        this.dynamic = dynamic;
        this.deprecated = deprecated;
        this.replacement = replacement;
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
    public Optional<Object> value()
    {
        return value;
    }

    @Nonnull
    public Optional<String> valueAsString()
    {
        return this.secret() ? Optional.of( Secret.OBSFUCATED ) : value.map( ConfigValue::valueToString );
    }

    @Override
    public String toString()
    {
        return valueAsString().orElse( "null" );
    }

    public boolean deprecated()
    {
        return deprecated;
    }

    @Nonnull
    public Optional<String> replacement()
    {
        return replacement;
    }

    public boolean internal()
    {
        return internal;
    }

    public boolean secret()
    {
        return secret;
    }

    public boolean dynamic()
    {
        return dynamic;
    }

    @Nonnull
    public Optional<String> documentedDefaultValue()
    {
        return documentedDefaultValue;
    }

    @Nonnull
    public String valueDescription()
    {
        return valueDescription;
    }

    static String valueToString( Object v )
    {
        if ( v instanceof Duration )
        {
            Duration d = (Duration) v;
            return String.format( "%dms", d.toMillis() );
        }
        return String.valueOf( v );
    }
}
