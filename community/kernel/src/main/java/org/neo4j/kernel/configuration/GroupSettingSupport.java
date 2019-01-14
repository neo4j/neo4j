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
package org.neo4j.kernel.configuration;

import org.neo4j.graphdb.config.Setting;

/**
 * This class helps you implement grouped settings without exposing internal utility methods
 * in public APIs - eg. this class is not public, and because you use delegation rather than
 * subclassing to use it, we don't end up exposing this class publicly.
 */
public class GroupSettingSupport
{
    private final String groupName;
    public final String groupKey;

    private static String groupPrefix( Class<?> groupClass )
    {
        return groupClass.getAnnotation( Group.class ).value();
    }

    public GroupSettingSupport( Class<?> groupClass, String groupKey )
    {
        this( groupPrefix( groupClass ), groupKey );
    }

    /**
     * @param groupPrefix the base that is common for each group of this kind, eg. 'dbms.mygroup'
     * @param groupKey the unique key for this particular group instance, eg. '0' or 'group1',
     *                 this gets combined with the groupPrefix to eg. `dbms.mygroup.0`
     */
    private GroupSettingSupport( String groupPrefix, String groupKey )
    {
        this.groupKey = groupKey;
        this.groupName = groupPrefix + "." + groupKey;
    }

    /**
     * Define a sub-setting of this group. The setting passed in should not worry about
     * the group prefix or key. If you want config like `dbms.mygroup.0.foo=bar`, you should
     * pass in a setting with the key `foo` here.
     */
    public <T> Setting<T> scope( Setting<T> setting )
    {
        setting.withScope( key -> groupName + "." + key );
        return setting;
    }
}
