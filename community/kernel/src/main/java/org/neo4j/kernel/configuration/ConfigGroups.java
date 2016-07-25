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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.Pair;

import static java.util.stream.Collectors.toList;

public class ConfigGroups
{
    /**
     * This mechanism can be used as an argument to {@link Config#view(Function)} to view a set of config options that
     * share a common base config key as a group.
     * This specific version handles multiple groups, so the common base key should be followed by a number denoting
     * the group, followed by the group config
     * values, eg:
     * <p>
     * {@code <base name>.<group key>.<config key>}
     * <p>
     * The config of each group can then be accessed as if the {@code config key} in the pattern above was the entire
     * config key. For example, given the
     * following configuration:
     * <p>
     * <pre>
     *     dbms.books.0.name=Hansel & Gretel
     *     dbms.books.0.author=JJ Abrams
     *     dbms.books.1.name=NKJV
     *     dbms.books.1.author=Jesus
     * </pre>
     * <p>
     * We can then access these config values as groups:
     * <p>
     * <pre>
     * {@code
     *     Setting<String> bookName = setting("name", STRING); // note that the key here is only 'name'
     *
     *     ConfigView firstBook = config.view( groups("dbms.books") ).get(0);
     *
     *     assert firstBook.get(bookName).equals("Hansel & Gretel");
     * }
     * </pre>
     *
     * @param baseName the base name for the groups, this will be the first part of the config key, followed by a
     *                 grouping number, followed by the group
     *                 config options
     * @return a list of grouped config options
     */
    public static Function<ConfigValues, List<Configuration>> groups( String baseName )
    {
        Pattern pattern = Pattern.compile( Pattern.quote( baseName ) + "\\.(\\d+)\\.(.+)" );

        return ( values ) -> {
            Map<String, Map<String, String>> groups = new HashMap<>();
            for ( Pair<String, String> entry : values.rawConfiguration() )
            {
                Matcher matcher = pattern.matcher( entry.first() );

                if ( matcher.matches() )
                {
                    String index = matcher.group( 1 );
                    String configName = matcher.group( 2 );
                    String value = entry.other();

                    Map<String, String> groupConfig = groups.get( index );
                    if ( groupConfig == null )
                    {
                        groupConfig = new HashMap<>();
                        groups.put( index, groupConfig );
                    }
                    groupConfig.put( configName, value );
                }
            }

            Function<Map<String, String>, Configuration> mapper = m -> new Configuration()
            {
                @Override
                public <T> T get( Setting<T> setting )
                {
                    return setting.apply( m::get );
                }
            };
            return groups.values().stream()
                    .map( mapper )
                    .collect( toList() );
        };
    }
}
