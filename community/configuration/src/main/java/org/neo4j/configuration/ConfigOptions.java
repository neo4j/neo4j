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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingGroup;

/**
 * Describes one or several configuration options.
 */
public class ConfigOptions
{
    private final SettingGroup<?> settingGroup;

    public ConfigOptions( @Nonnull SettingGroup<?> settingGroup )
    {
        this.settingGroup = settingGroup;
    }

    @Nonnull
    public SettingGroup<?> settingGroup()
    {
        return settingGroup;
    }

    @Nonnull
    public List<ConfigValue> asConfigValues( @Nonnull Map<String,String> validConfig )
    {
        Map<String,Setting<?>> settings = settingGroup.settings( validConfig ).stream()
                .collect( Collectors.toMap( Setting::name, s -> s ) );

        return settingGroup.values( validConfig ).entrySet().stream()
                .map( val ->
                {
                    BaseSetting<?> setting = (BaseSetting) settings.get( val.getKey() );
                    return new ConfigValue( setting.name(), setting.description(),
                            setting.documentedDefaultValue(),
                        Optional.ofNullable( val.getValue() ),
                            setting.valueDescription(), setting.internal(), setting.dynamic(),
                            setting.deprecated(), setting.replacement(), setting.secret() );
                } )
                .collect( Collectors.toList() );
    }
}
