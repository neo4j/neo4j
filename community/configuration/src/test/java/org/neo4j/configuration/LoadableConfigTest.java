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

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingGroup;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class LoadableConfigTest
{
    @Test
    public void getConfigOptions()
    {
        Map<String,String> config = stringMap(
                TestConfig.integer.name(), "123",
                TestConfig.string.name(), "bah",
                TestConfig.oldString.name(), "moo",
                TestConfig.dynamic.name(), "foo" );

        TestConfig testSettings = new TestConfig();

        List<ConfigOptions> options = testSettings.getConfigOptions();

        assertEquals( 4, options.size() );

        SettingGroup<?> integerSetting = options.get( 0 ).settingGroup();
        assertEquals( 1, integerSetting.values( emptyMap() ).get( TestConfig.integer.name() ) );
        assertEquals( 123, integerSetting.values( config ).get( TestConfig.integer.name() ) );
        assertEquals( Optional.empty(), integerSetting.description() );
        assertFalse( integerSetting.deprecated() );
        assertFalse( integerSetting.dynamic() );
        assertEquals( Optional.empty(), integerSetting.replacement() );

        SettingGroup<?> stringSetting = options.get( 1 ).settingGroup();
        assertEquals( "bob", stringSetting.values( emptyMap() ).get( TestConfig.string.name() ) );
        assertEquals( "bah", stringSetting.values( config ).get( TestConfig.string.name() ) );
        assertEquals( "A string setting", stringSetting.description().get() );
        assertFalse( stringSetting.deprecated() );
        assertFalse( stringSetting.dynamic() );
        assertEquals( Optional.empty(), stringSetting.replacement() );

        SettingGroup<?> oldStringSetting = options.get( 2 ).settingGroup();
        assertEquals( "tim", oldStringSetting.values( emptyMap() ).get( TestConfig.oldString.name() ) );
        assertEquals( "moo", oldStringSetting.values( config ).get( TestConfig.oldString.name() ) );
        assertEquals( "A deprecated string setting", oldStringSetting.description().get() );
        assertTrue( oldStringSetting.deprecated() );
        assertFalse( oldStringSetting.dynamic() );
        assertEquals( TestConfig.string.name(), oldStringSetting.replacement().get() );

        SettingGroup<?> dynamicSetting = options.get( 3 ).settingGroup();
        assertEquals( "defaultDynamic", dynamicSetting.values( emptyMap() ).get( TestConfig.dynamic.name() ) );
        assertEquals( "foo", dynamicSetting.values( config ).get( TestConfig.dynamic.name() ) );
        assertEquals( "A dynamic string setting", dynamicSetting.description().get() );
        assertFalse( dynamicSetting.deprecated() );
        assertTrue( dynamicSetting.dynamic() );
        assertEquals( Optional.empty(), dynamicSetting.replacement() );
    }

    private static class TestConfig implements LoadableConfig
    {
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

        @Description( "A dynamic string setting" )
        @Dynamic
        public static final Setting<String> dynamic = new StringSetting()
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
                return "myDynamicProperty";
            }

            @Override
            public void withScope( Function<String,String> scopingRule )
            {

            }

            @Override
            public String getDefaultValue()
            {
                return "defaultDynamic";
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
