/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.configuration;

import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.JVM_ADDITIONAL;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.STRING;

import java.nio.file.Path;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;

@ServiceProvider
public class BootloaderSettings implements SettingsDeclaration {
    @Description(
            "Name of the Windows Service managing Neo4j when installed using `neo4j install-service`. Only applicable on Windows OS."
                    + " Note: This must be unique for each individual installation.")
    public static final Setting<String> windows_service_name =
            newBuilder("server.windows_service_name", STRING, "neo4j").build();

    @Internal
    @Description("Path of the lib directory")
    public static final Setting<Path> windows_tools_directory = newBuilder(
                    "internal.server.directories.windows_tools", PATH, Path.of("bin", "tools"))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    @Description("Additional JVM arguments. Please note that argument order can be significant.")
    public static final Setting<String> additional_jvm =
            newBuilder("server.jvm.additional", JVM_ADDITIONAL, null).build();

    @Description("Initial heap size. By default it is calculated based on available system resources.")
    public static final Setting<Long> initial_heap_size =
            newBuilder("server.memory.heap.initial_size", BYTES, null).build();

    @Description("Maximum heap size. By default it is calculated based on available system resources.")
    public static final Setting<Long> max_heap_size =
            newBuilder("server.memory.heap.max_size", BYTES, null).build();

    @Description("GC Logging Options")
    public static final Setting<String> gc_logging_options = newBuilder(
                    "server.logs.gc.options", STRING, "-Xlog:gc*,safepoint,age*=trace")
            .build();

    @Description("Number of GC logs to keep.")
    public static final Setting<Integer> gc_logging_rotation_keep_number =
            newBuilder("server.logs.gc.rotation.keep_number", INT, 5).build();

    @Description("Size of each GC log that is kept.")
    public static final Setting<Long> gc_logging_rotation_size = newBuilder(
                    "server.logs.gc.rotation.size", BYTES, ByteUnit.mebiBytes(20))
            .build();

    @Description("Enable GC Logging")
    public static final Setting<Boolean> gc_logging_enabled =
            newBuilder("server.logs.gc.enabled", BOOL, false).build();

    @Description("Path of the run directory. This directory holds Neo4j's runtime state, such as a pidfile when it "
            + "is running in the background. The pidfile is created when starting neo4j and removed when stopping it."
            + " It may be placed on an in-memory filesystem such as tmpfs.")
    public static final Setting<Path> run_directory = newBuilder("server.directories.run", PATH, Path.of("run"))
            .setDependency(neo4j_home)
            .immutable()
            .build();

    @Internal
    @Description("Path of the pid file.")
    public static final Setting<Path> pid_file = newBuilder(
                    "internal.server.directories.pid_file", PATH, Path.of("neo4j.pid"))
            .setDependency(run_directory)
            .immutable()
            .build();

    @Description("Path of the lib directory")
    public static final Setting<Path> lib_directory = newBuilder("server.directories.lib", PATH, Path.of("lib"))
            .setDependency(neo4j_home)
            .immutable()
            .build();
}
