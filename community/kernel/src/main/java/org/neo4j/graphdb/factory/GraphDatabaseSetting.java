/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.net.URI;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Settings;

/**
 * Setting types for Neo4j. Actual settings are in GraphDatabaseSettings.
 * <p/>
 * This is a usage-only class, backwards compatibility is retained for using implementations
 * of it, but not for implementing it.
 */
@Deprecated
public abstract class GraphDatabaseSetting<T> implements Setting<T>
{
    public static final String TRUE = "true";
    public static final String FALSE = "false";

    public static final String ANY = ".+";

    public static final String SIZE = "\\d+[kmgKMG]";

    public static final String DURATION = "\\d+(ms|s|m)";

    //
    // Implementations of GraphDatabaseSetting
    //

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class StringSetting
            extends SettingWrapper<String>
    {
        public StringSetting( Setting<String> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class IntegerSetting
            extends SettingWrapper<Integer>
    {
        public IntegerSetting( Setting<Integer> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class LongSetting
            extends SettingWrapper<Long>
    {
        public LongSetting( Setting<Long> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class FloatSetting
            extends SettingWrapper<Float>
    {
        public FloatSetting( Setting<Float> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class DoubleSetting
            extends SettingWrapper<Double>
    {
        public DoubleSetting( Setting<Double> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class PortSetting
            extends IntegerSetting
    {
        public PortSetting( Setting<Integer> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class HostnamePortSetting
            extends SettingWrapper<HostnamePort>
    {
        public HostnamePortSetting( Setting<HostnamePort> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class TimeSpanSetting extends SettingWrapper<Long>
    {
        public TimeSpanSetting( Setting<Long> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class OptionsSetting extends SettingWrapper<String>
    {
        public OptionsSetting( Setting<String> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class BooleanSetting
        extends SettingWrapper<Boolean>
    {
        public BooleanSetting( Setting<Boolean> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class FileSetting
            extends SettingWrapper<File>
    {
        public FileSetting( Setting<File> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class DirectorySetting
            extends SettingWrapper<File>
    {
        public DirectorySetting( Setting<File> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class NumberOfBytesSetting
            extends SettingWrapper<Long>
    {
        public NumberOfBytesSetting( Setting<Long> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class IntegerRangeNumberOfBytesSetting extends SettingWrapper<Integer>
    {
        public IntegerRangeNumberOfBytesSetting( Setting<Integer> setting )
        {
            super( setting );
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    public static class URISetting extends SettingWrapper<URI>
    {
        public URISetting( Setting<URI> setting )
        {
            super( setting );
        }
    }

    /**
     * Wrapper of Setting<T> created by Setttings.setting method.
     *
     * This should go away when we can delete this class due to deprecation
     *
     *
     * @param <T>
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public static class SettingWrapper<T> extends GraphDatabaseSetting<T>
    {
        private Setting<T> setting;

        public SettingWrapper( Setting<T> setting )
        {
            this.setting = setting;
        }

        @Override
        public String name()
        {
            return setting.name();
        }

        @Override
        public String getDefaultValue()
        {
            return setting.getDefaultValue();
        }

        @Override
        public T apply( Function<String, String> settings )
        {
            return setting.apply( settings );
        }

        @Override
        public String toString()
        {
            return setting.toString();
        }
    }

    @Deprecated
    public static boolean osIsWindows()
    {
        return Settings.osIsWindows();
    }

    @Deprecated
    public static boolean osIsMacOS()
    {
        return Settings.osIsMacOS();
    }
}