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
import java.util.function.Function;

/**
 * An abstraction over external environment where {@link Neo4jAdminCommand}
 * and {@link Neo4jCommand} are executed. The main purpose of this abstraction is to
 * make the commands testable. {@link #SYSTEM} should be used outside tests.
 */
public record Environment(
        PrintStream out,
        PrintStream err,
        Function<String, String> envLookup,
        Function<String, String> propLookup,
        Runtime.Version version) {

    public static final Environment SYSTEM =
            new Environment(System.out, System.err, System::getenv, System::getProperty, Runtime.version());

    /**
     * This is sent by the child process when it is ready to fend for itself.
     * When running in service mode, this implies that the parent process can detach.
     */
    public static final char FULLY_FLEDGED = '\u0006'; // <ACK>
}
