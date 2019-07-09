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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.Settings.BOOLEAN;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.pathSetting;
import static org.neo4j.configuration.Settings.setting;

/**
 * This class holds settings which are used external to the java code. This includes things present in the
 * configuration which are only read and used by the wrapper scripts. By including them here, we suppress warning
 * messages about Unknown configuration options, and make it possible to document these options via the normal methods.
 *
 * Be aware that values are still validated.
 */
@SuppressWarnings( "unused" )
@ServiceProvider
public class ExternalSettings implements LoadableConfig
{
    @Description( "Name of the Windows Service." )
    public static final Setting<String> windowsServiceName = dummySetting( "dbms.windows_service_name",
            "neo4j" );
    @Description( "Additional JVM arguments. Argument order can be significant. To use a Java commercial feature, the argument to unlock " +
            "commercial features must precede the argument to enable the specific feature in the config value string. For example, " +
            "to use Flight Recorder, `-XX:+UnlockCommercialFeatures` must come before `-XX:+FlightRecorder`." )
    public static final Setting<String> additionalJvm = dummySetting( "dbms.jvm.additional" );

    @Description( "Initial heap size. By default it is calculated based on available system resources." )
    public static final Setting<String> initialHeapSize = dummySetting( "dbms.memory.heap.initial_size",
            "", "a byte size (valid units are `k`, `K`, `m`, `M`, `g`, `G`)" );

    @Description( "Maximum heap size. By default it is calculated based on available system resources." )
    public static final Setting<String> maxHeapSize = dummySetting( "dbms.memory.heap.max_size", "",
            "a byte size (valid units are `k`, `K`, `m`, `M`, `g`, `G`)" );

    @Description( "GC Logging Options" )
    public static final Setting<String> gc_logging_options = dummySetting( "dbms.logs.gc.options" );

    @Description( "Number of GC logs to keep." )
    public static final Setting<String> gc_logging_rotation_keep_number =
            dummySetting( "dbms.logs.gc.rotation.keep_number", "0" );

    @Description( "Size of each GC log that is kept." )
    public static final Setting<String> gc_logging_rotation_size = dummySetting( "dbms.logs.gc.rotation.size", "" );

    @Description( "Enable GC Logging" )
    public static final Setting<Boolean> gc_logging_enabled = setting( "dbms.logs.gc.enabled", BOOLEAN, FALSE);

    @Description( "Path of the run directory. This directory holds Neo4j's runtime state, such as a pidfile when it " +
            "is running in the background. The pidfile is created when starting neo4j and removed when stopping it." +
            " It may be placed on an in-memory filesystem such as tmpfs." )
    public static final Setting<File> run_directory = pathSetting( "dbms.directories.run", "run" );

    @Description( "Path of the lib directory" )
    public static final Setting<File> lib_directory = pathSetting( "dbms.directories.lib", "lib" );

    private static DummySetting dummySetting( String name )
    {
        return new DummySetting( name, "", "a string" );
    }

    private static DummySetting dummySetting( String name, String defVal )
    {
        return new DummySetting( name, defVal, "a string" );
    }

    private static DummySetting dummySetting( String name, String defVal, String valDesc )
    {
        return new DummySetting( name, defVal, valDesc );
    }

    static class DummySetting extends BaseSetting<String>
    {

        private final String name;
        private final String defaultValue;
        private final String valueDescription;

        DummySetting( String name, String defVal, String valueDescription )
        {
            this.name = name;
            this.defaultValue = defVal;
            this.valueDescription = valueDescription;
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public void withScope( Function<String,String> scopingRule )
        {

        }

        @Override
        public String getDefaultValue()
        {
            return defaultValue;
        }

        @Override
        public String from( Configuration config )
        {
            return config.get( this );
        }

        @Override
        public String apply( Function<String,String> provider )
        {
            return provider.apply( name );
        }

        @Override
        public List<Setting<String>> settings( Map<String,String> params )
        {
            return Collections.singletonList( this );
        }

        @Override
        public String valueDescription()
        {
            return valueDescription;
        }
    }
}
