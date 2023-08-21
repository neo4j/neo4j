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
package org.neo4j.server.startup;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * Execution context that has the ability to create a DBMS bootloader.
 * This execution context is useful for admin commands that work with the DBMS process.
 * In fact, every execution context created by {@link Neo4jAdminCommand} and {@link Neo4jCommand} is
 * Enhanced execution context, but most commands don't need to know that.
 */
public class EnhancedExecutionContext extends ExecutionContext {

    private final Supplier<Bootloader.Dbms> dbmsBootloaderFactory;

    private final ClassLoader classloaderWithPlugins;

    public EnhancedExecutionContext(
            Path homeDir,
            Path confDir,
            PrintStream out,
            PrintStream err,
            FileSystemAbstraction fs,
            Supplier<Bootloader.Dbms> dbmsBootloaderFactory,
            ClassLoader pluginClassLoader) {
        super(homeDir, confDir, out, err, fs);

        this.dbmsBootloaderFactory = dbmsBootloaderFactory;
        this.classloaderWithPlugins = pluginClassLoader;
    }

    Bootloader.Dbms createDbmsBootloader() {
        return dbmsBootloaderFactory.get();
    }

    public ClassLoader getClassloaderWithPlugins() {
        return classloaderWithPlugins;
    }

    public static EnhancedExecutionContext unwrapFromExecutionContext(ExecutionContext executionContext) {
        if (executionContext instanceof EnhancedExecutionContext) {
            return (EnhancedExecutionContext) executionContext;
        }

        throw new IllegalStateException("The supplied execution context is not an enhanced execution context");
    }
}
