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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.SettingGroup;

/**
 * Describes one or several configuration options.
 */
public class ConfigOptions
{
    private final SettingGroup<?> settingGroup;
    private final Optional<String> description;
    private final Optional<String> documentedDefaultValue;
    private final boolean deprecated;
    private final Optional<String> replacement;

    public ConfigOptions( @Nonnull SettingGroup<?> settingGroup, @Nonnull Optional<String> description,
            @Nonnull Optional<String> documentedDefaultValue, boolean deprecated,
            @Nonnull Optional<String> replacement )
    {
        this.settingGroup = settingGroup;
        this.description = description;
        this.documentedDefaultValue = documentedDefaultValue;
        this.deprecated = deprecated;
        this.replacement = replacement;
    }

    @Nonnull
    public SettingGroup<?> settingGroup()
    {
        return settingGroup;
    }

    @Nonnull
    public Optional<String> description()
    {
        return description;
    }

    @Nonnull
    public Optional<String> documentedDefaultValue()
    {
        return documentedDefaultValue;
    }

    @Nonnull
    public List<ConfigValue> asConfigValues( @Nonnull Map<String,String> validConfig )
    {
        return settingGroup.values( validConfig ).entrySet().stream()
                .map( val -> new ConfigValue( val.getKey(), description(), Optional.ofNullable( val.getValue() ),
                        deprecated, replacement ) )
                .collect( Collectors.toList() );
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
}
