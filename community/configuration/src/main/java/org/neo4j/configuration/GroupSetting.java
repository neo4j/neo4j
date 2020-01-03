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

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.Service;

/**
 * A group of settings that can represented in multiple instances.
 * In comparison with {@link SettingsDeclaration} where each setting only exists in a single instance
 */
@Service
@PublicApi
public abstract class GroupSetting
{
    private final String name;

    protected GroupSetting( String name )
    {
        this.name = name;
    }

    /**
     * The name is unique to one instance of a group
     *
     * @return the name of this group
     */
    public final String name()
    {
        return name;
    }

    /**
     * The prefix is the same for all the settings in this group
     *
     * @return the prefix for this group
     */
    public abstract String getPrefix();

    /**
     * Helper method when creating settings for this group.
     * This is the preferred method if the group contains multiple settings.
     *
     * @param suffix The unique name of the setting to be created
     * @param parser The parser to be used in the setting
     * @param defaultValue The default value to be associated with the setting
     * @param <T> the type of the objects represented by the setting
     * @return the builder of the setting
     */
    protected <T> SettingImpl.Builder<T> getBuilder( String suffix, SettingValueParser<T> parser, T defaultValue )
    {
        return SettingImpl.newBuilder( String.format( "%s.%s.%s", getPrefix(), name, suffix ), parser, defaultValue );
    }

    /**
     * Helper method when creating settings for this group.
     * This is the preferred method if the group contains only a single setting.
     *
     * @param parser The parser to be used in the setting
     * @param defaultValue The default value to be associated with the setting
     * @param <T> the type of the objects represented by the setting
     * @return the builder of the setting
     */
    protected <T> SettingImpl.Builder<T> getBuilder( SettingValueParser<T> parser, T defaultValue )
    {
        return SettingImpl.newBuilder( String.format( "%s.%s", getPrefix(), name ), parser, defaultValue );
    }
}
