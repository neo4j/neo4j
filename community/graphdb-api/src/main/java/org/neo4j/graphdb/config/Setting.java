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
package org.neo4j.graphdb.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Settings that can be provided in configurations are represented by instances of this interface, and are available
 * as static fields in various *Settings classes.
 * <p>
 * This interface is available only for use, not for implementing. Implementing this interface is not expected, and
 * backwards compatibility is not guaranteed for implementors.
 *
 * @param <T> type of value this setting will parse input string into and return.
 * @deprecated The settings API will be completely rewritten in 4.0
 */
@Deprecated
public interface Setting<T> extends Function<Function<String,String>,T>, SettingValidator, SettingGroup<T>
{
    /**
     * Get the name of the setting. This typically corresponds to a key in a properties file, or similar.
     *
     * @return the name
     */
    String name();

    /**
     * Make this setting bound to a scope
     *
     * @param scopingRule The scoping rule to be applied to this setting
     */
    void withScope( Function<String,String> scopingRule );

    /**
     * Get the default value of this setting, as a string.
     *
     * @return the default value
     */
    String getDefaultValue();

    @Deprecated
    T from( Configuration config );

    @Override
    default Map<String,T> values( Map<String,String> validConfig )
    {
        return singletonMap( name(), apply( validConfig::get ) );
    }

    @Override
    default Map<String,String> validate( Map<String,String> rawConfig, Consumer<String> warningConsumer )
            throws InvalidSettingException
    {
        // Validate setting, if present or default value otherwise
        try
        {
            apply( rawConfig::get );
            // only return if it was present though
            if ( rawConfig.containsKey( name() ) )
            {
                return stringMap( name(), rawConfig.get( name() ) );
            }
            else
            {
                return emptyMap();
            }
        }
        catch ( RuntimeException e )
        {
            throw new InvalidSettingException( e.getMessage(), e );
        }
    }

    @Override
    default List<Setting<T>> settings( Map<String,String> params )
    {
        return Collections.singletonList( this );
    }

    /**
     * Get the function used to parse this setting.
     *
     * @return the parser function
     */
    default Optional<Function<String,T>> getParser()
    {
        return Optional.empty();
    }
}
