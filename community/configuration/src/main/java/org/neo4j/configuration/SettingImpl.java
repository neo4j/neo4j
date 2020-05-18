/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.annotations.api.IgnoreApiCheck;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.Exceptions;

import static java.lang.String.format;

@IgnoreApiCheck
public final class SettingImpl<T> implements Setting<T>
{
    private final String name;
    private final String suffix;
    private final SettingImpl<T> dependency;
    private final SettingValueParser<T> parser;
    private final T defaultValue;
    private final List<SettingConstraint<T>> constraints;
    private final boolean dynamic;
    private final boolean immutable;
    private boolean internal;
    private String description;
    private boolean deprecated;
    private String documentedDefaultValue;

    private SettingImpl( String name,
            SettingValueParser<T> parser,
            T defaultValue,
            List<SettingConstraint<T>> constraints,
            boolean dynamic,
            boolean immutable,
            SettingImpl<T> dependency )
    {
        this.name = name;
        this.parser = parser;
        this.dependency = dependency;
        this.constraints = constraints;
        this.defaultValue = defaultValue;
        this.dynamic = dynamic;
        this.immutable = immutable;
        this.internal = name != null && name.contains( "unsupported." );
        this.suffix = StringUtils.isNotEmpty( name ) ? name.substring( name.lastIndexOf( '.' ) + 1 ) : name;
    }

    public static <T> Builder<T> newBuilder( String name, SettingValueParser<T> parser, T defaultValue )
    {
        return new Builder<>( name, parser, defaultValue );
    }

    @Override
    public T defaultValue()
    {
        return defaultValue;
    }

    public T parse( String value )
    {
        if ( value == null )
        {
           return null;
        }

        return parser.parse( value );
    }

    public String valueToString( T value )
    {
        if ( value != null )
        {
            return parser.valueToString( value );
        }
        return "No Value";
    }

    T solveDefault( T value, T defaultValue )
    {
        return parser.solveDefault(value, defaultValue);
    }

    T solveDependency( T value, T dependencyValue )
    {
        return parser.solveDependency( value, dependencyValue );
    }

    public void validate( T value, Configuration config )
    {
        if ( value != null )
        {
            if ( !parser.getType().isAssignableFrom( value.getClass() ) ) //Does only check outer class if generic types.
            {
                throw new IllegalArgumentException( format( "Setting '%s' can not have value '%s'. Should be of type '%s', but is '%s'",
                                name, value, parser.getType().getSimpleName(), value.getClass().getSimpleName() ) );
            }
            try
            {
                parser.validate( value );
                for ( SettingConstraint<T> constraint : constraints )
                {
                    constraint.validate( value, config );
                }
            }
            catch ( IllegalArgumentException e )
            {
                throw Exceptions.withMessage( e, format( "Failed to validate '%s' for '%s': %s", value, name(), e.getMessage() ) );
            }

        }
    }

    @Override
    public String toString()
    {
        String desc = format( "%s, %s", name, parser.getDescription() );

        if ( !constraints.isEmpty() )
        {
            String constraintDesc = constraints.stream().map( SettingConstraint::getDescription ).collect( Collectors.joining( " and " ) );
            desc = format( "%s which %s", desc, constraintDesc );
        }

        if ( dependency != null )
        {
            desc = format( "%s. %s from %s", desc, parser.getSolverDescription(), dependency.name() );
        }

        return desc;
    }

    SettingImpl<T> dependency()
    {
        return dependency;
    }

    @Override
    public String description()
    {
        return description != null ? description : toString();
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
        SettingImpl<?> setting = (SettingImpl<?>) o;
        return name.equals( setting.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
    }

    @Override
    public String name()
    {
        return name;
    }

    public String suffix()
    {
        return suffix;
    }

    @Override
    public boolean dynamic()
    {
        return dynamic;
    }

    public boolean immutable()
    {
        return immutable;
    }

    @Override
    public boolean internal()
    {
        return internal;
    }

    public boolean deprecated()
    {
        return deprecated;
    }

    public String documentedDefaultValue()
    {
        return documentedDefaultValue;
    }

    void setDescription( String description )
    {
        this.description = description;
    }

    void setInternal()
    {
        internal = true;
    }

    void setDeprecated()
    {
        deprecated = true;
    }

    void setDocumentedDefaultValue( String documentedDefaultValue )
    {
        this.documentedDefaultValue = documentedDefaultValue;
    }

    SettingValueParser<T> parser()
    {
        return parser;
    }

    public static class Builder<T>
    {
        private final String name;
        private final SettingValueParser<T> parser;
        private final List<SettingConstraint<T>> constraints = new ArrayList<>();
        private final T defaultValue;
        private boolean dynamic;
        private boolean immutable;
        private SettingImpl<T> dependency;

        private Builder( String name, SettingValueParser<T> parser, T defaultValue )
        {
            this.name = name;
            this.parser = parser;
            this.defaultValue = defaultValue;
        }

        public Builder<T> dynamic()
        {
            this.dynamic = true;
            return this;
        }

        public Builder<T> immutable()
        {
            this.immutable = true;
            return this;
        }

        public Builder<T> addConstraint( SettingConstraint<T> constraint )
        {
            constraint.setParser( parser );
            constraints.add( constraint );
            return this;
        }

        public Builder<T> setDependency( Setting<T> setting )
        {
            dependency = (SettingImpl<T>) setting;
            return this;
        }

        public Setting<T> build()
        {
            if ( immutable && dynamic )
            {
                throw new IllegalArgumentException( "Setting can not be both dynamic and immutable" );
            }
            if ( dependency != null && !dependency.immutable() )
            {
                throw new IllegalArgumentException( "Setting can only have immutable dependency" );
            }

            return new SettingImpl<>( name, parser, defaultValue, constraints, dynamic, immutable, dependency );
        }
    }
}
