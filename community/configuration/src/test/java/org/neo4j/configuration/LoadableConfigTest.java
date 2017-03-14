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

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.MapUtil;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LoadableConfigTest
{
    @Test
    public void getConfigOptions() throws Exception
    {
        Map<String,String> config = MapUtil.stringMap( "myInt", "123", "myString", "bah", "myOldString", "moo" );

        TestConfig testSettings = new TestConfig();

        List<ConfigOptions> options = testSettings.getConfigOptions();

        assertEquals( 3, options.size() );

        assertEquals( 1, options.get( 0 ).settingGroup().values( emptyMap() ).get( "myInt" ) );
        assertEquals( 123, options.get( 0 ).settingGroup().values( config ).get( "myInt" ) );
        assertEquals( Optional.empty(), options.get( 0 ).settingGroup().description() );
        assertFalse( options.get(0).settingGroup().deprecated() );
        assertEquals( Optional.empty(), options.get( 0 ).settingGroup().replacement() );

        assertEquals( "bob", options.get( 1 ).settingGroup().values( emptyMap() ).get( "myString" ) );
        assertEquals( "bah", options.get( 1 ).settingGroup().values( config ).get( "myString" ) );
        assertEquals( "A string setting", options.get( 1 ).settingGroup().description().get() );
        assertFalse( options.get(1).settingGroup().deprecated() );
        assertEquals( Optional.empty(), options.get( 1 ).settingGroup().replacement() );

        assertEquals( "tim", options.get( 2 ).settingGroup().values( emptyMap() ).get( "myOldString" ) );
        assertEquals( "moo", options.get( 2 ).settingGroup().values( config ).get( "myOldString" ) );
        assertEquals( "A deprecated string setting", options.get( 2 ).settingGroup().description().get() );
        assertTrue( options.get(2).settingGroup().deprecated() );
        assertEquals( "myString", options.get( 2 ).settingGroup().replacement().get() );
    }

    private static class TestConfig implements LoadableConfig
    {
        @SuppressWarnings( "unused" )
        public static final Setting<Integer> integer = new BaseSetting<Integer>()
        {
            @Override
            public String valueDescription()
            {
                return "an Integer";
            }

            @Override
            public String name()
            {
                return "myInt";
            }

            @Override
            public void withScope( Function<String,String> scopingRule )
            {

            }

            @Override
            public String getDefaultValue()
            {
                return "1";
            }

            @Override
            public Integer from( Configuration config )
            {
                return config.get( this );
            }

            @Override
            public Integer apply( Function<String,String> provider )
            {
                String val = provider.apply( name() );
                if ( val == null )
                {
                    val = getDefaultValue();
                }
                return Integer.parseInt( val );
            }

        };

        @SuppressWarnings( "unused" )
        @Description( "A string setting" )
        public static final Setting<String> string = new StringSetting()
        {
            @Override
            public String apply( Function<String,String> provider )
            {
                String val = provider.apply( name() );
                if ( val == null )
                {
                    val = getDefaultValue();
                }
                return val;
            }

            @Override
            public String name()
            {
                return "myString";
            }

            @Override
            public void withScope( Function<String,String> function )
            {

            }

            @Override
            public String getDefaultValue()
            {
                return "bob";
            }

            @Override
            public String from( Configuration configuration )
            {
                return configuration.get( this );
            }
        };

        @SuppressWarnings( "unused" )
        @Description( "A deprecated string setting" )
        @Deprecated
        @ReplacedBy( "myString" )
        public static final Setting<String> oldString = new StringSetting()
        {
            @Override
            public String apply( Function<String,String> provider )
            {
                String val = provider.apply( name() );
                if ( val == null )
                {
                    val = getDefaultValue();
                }
                return val;
            }

            @Override
            public String name()
            {
                return "myOldString";
            }

            @Override
            public void withScope( Function<String,String> function )
            {

            }

            @Override
            public String getDefaultValue()
            {
                return "tim";
            }

            @Override
            public String from( Configuration configuration )
            {
                return configuration.get( this );
            }
        };

        @SuppressWarnings( "unused" )
        @Description( "A private setting which is not accessible" )
        private static final Setting<String> ignoredSetting = new StringSetting()
        {
            @Override
            public String apply( Function<String,String> provider )
            {
                return provider.apply( name() );
            }

            @Override
            public String name()
            {
                return "myString";
            }

            @Override
            public void withScope( Function<String,String> function )
            {

            }

            @Override
            public String getDefaultValue()
            {
                return "bob";
            }

            @Override
            public String from( Configuration configuration )
            {
                return configuration.get( this );
            }
        };
    }

    private abstract static class StringSetting extends BaseSetting<String>
    {

        @Override
        public String valueDescription()
        {
            return "a String";
        }
    }
}
