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
package org.neo4j.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;

/**
 * Convenience for building config for a test database.
 * In particular this is class is useful when parameterizing a test with different configurations.
 * <p>
 * Usage:
 * <pre><code>
 *  import static org.neo4j.test.ConfigBuilder.configure;
 *
 * {@literal @}{@link org.junit.runner.RunWith RunWith}({@link org.junit.runners.Parameterized Parameterized.class})
 *  public class SomeTest
 *  {
 *     {@literal @}{@link org.junit.runners.Parameterized.Parameters Parameterized.Parameters}( name = "{0}" )
 *      public static Iterable&lt;Object[]&gt; configurations()
 *      {
 *          return Arrays.asList(
 *              // First set of configuration
 *              {@link #configure(Setting, String) configure}( {@link
 *              org.neo4j.graphdb.factory.GraphDatabaseSettings#query_cache_size
 *              GraphDatabaseSettings.query_cache_size}, "42" ).{@link #asParameters() asParameters}(),
 *              // Second set of configuration
 *              {@link #configure(Setting, String) configure}( {@link
 *              org.neo4j.graphdb.factory.GraphDatabaseSettings#query_cache_size
 *              GraphDatabaseSettings.query_cache_size}, "12" )
 *                   .{@link #and(Setting, String) and}( {@link
 *                   org.neo4j.graphdb.factory.GraphDatabaseSettings#cypher_min_replan_interval
 *                   GraphDatabaseSettings.cypher_min_replan_interval}, "5000" ).{@link #asParameters() asParameters}()
 *          );
 *      }
 *
 *      public final{@literal @}Rule {@link org.neo4j.test.rule.DatabaseRule DatabaseRule} db;
 *
 *      public SomeTest( ConfigBuilder config )
 *      {
 *          this.db = new {@link org.neo4j.test.rule.ImpermanentDatabaseRule
 *          ImpermanentDatabaseRule}().{@link org.neo4j.test.rule.DatabaseRule#withConfiguration(Map)
 *          withConfiguration}( config.{@link #configuration() configuration}() );
 *      }
 *  }
 * </code></pre>
 */
public final class ConfigBuilder
{
    public static ConfigBuilder configure( Setting<?> key, String value )
    {
        Map<Setting<?>,String> config = new HashMap<>();
        config.put( key, value );
        return new ConfigBuilder( config );
    }

    private final Map<Setting<?>,String> config;

    private ConfigBuilder( Map<Setting<?>,String> config )
    {
        this.config = config;
    }

    public Map<Setting<?>,String> configuration()
    {
        return Collections.unmodifiableMap( config );
    }

    public ConfigBuilder and( Setting<?> key, String value )
    {
        Map<Setting<?>,String> config = new HashMap<>( this.config );
        config.put( key, value );
        return new ConfigBuilder( config );
    }

    public Object[] asParameters()
    {
        return new Object[] {this};
    }

    @Override
    public String toString()
    {
        return config.toString();
    }
}
