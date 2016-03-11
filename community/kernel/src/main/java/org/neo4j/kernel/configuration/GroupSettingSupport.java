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
package org.neo4j.kernel.configuration;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;

/**
 * This class helps you implement grouped settings without exposing internal utility methods
 * in public APIs - eg. this class is not public, and because you use delegation rather than
 * subclassing to use it, we don't end up exposing this class publicly.
 */
public class GroupSettingSupport
{
    private final String key;
    private final String prefix;

    /**
     * List all keys for a given group type, this is a way to enumerate all instances of a group
     * in a given configuration.
     *
     * @param settingsGroup the group to find all keys of
     * @return a function that can be applied to {@link Config#view(Function)} to list all keys
     */
    public static Function<ConfigValues,Stream<String>> enumerate( Class<?> settingsGroup )
    {
        Pattern pattern = Pattern.compile(
                Pattern.quote( groupPrefix( settingsGroup ) ) + "\\.([^\\.]+)\\.(.+)" );
        return ( values ) -> values.rawConfiguration().stream()
                // Find all config options that start with the group prefix
                .map( entry -> pattern.matcher( entry.first() ) )
                .filter( Matcher::matches )
                // Extract the group key used in each of those settings
                .map( match -> match.group( 1 ) )
                // Deduplicate and sort
                .distinct().sorted();
    }

    private static String groupPrefix( Class<?> groupClass )
    {
        return groupClass.getAnnotation( Group.class ).value();
    }

    public GroupSettingSupport( Class<?> groupClass, Object groupKey )
    {
        this( groupPrefix( groupClass ), groupKey );
    }

    /**
     * @param groupPrefix the base that is common for each group of this kind, eg. 'dbms.mygroup'
     * @param groupKey the unique key for this particular group instance, eg. '0' or 'group1',
     *                 this gets combined with the groupPrefix to eg. `dbms.mygroup.0`
     */
    public GroupSettingSupport( String groupPrefix, Object groupKey )
    {
        this.prefix = groupPrefix;
        this.key = String.format( "%s.%s", prefix, groupKey );
    }

    /**
     * Define a sub-setting of this group. The setting passed in should not worry about
     * the group prefix or key. If you want config like `dbms.mygroup.0.foo=bar`, you should
     * pass in a setting with the key `foo` here.
     */
    public <T> Setting<T> scope( Setting<T> inner )
    {
        return new Setting<T>()
        {
            @Override
            public String name()
            {
                return scopeToGroup( inner.name() );
            }

            @Override
            public String getDefaultValue()
            {
                return inner.getDefaultValue();
            }

            @Override
            public T from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public T apply( Function<String,String> config )
            {
                return inner.apply( ( key ) -> config.apply( scopeToGroup( key ) ) );
            }

            @Override
            public String toString()
            {
                return inner.toString();
            }

            @Override
            public int hashCode()
            {
                return name().hashCode();
            }

            @Override
            public boolean equals( Object obj )
            {
                return obj != null
                       && obj instanceof Setting
                       && name().equals( ((Setting) obj).name() );
            }

            private String scopeToGroup( String key )
            {
                return String.format( "%s.%s", GroupSettingSupport.this.key, key );
            }
        };
    }
}
