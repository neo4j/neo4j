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
package org.neo4j.server;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.server.startup.EntryPoint;

@ServiceProvider
public class CommunityEntryPoint implements EntryPoint {
    private static Bootstrapper bootstrapper;

    public static void main(String[] args) {
        int status = NeoBootstrapper.start(new CommunityBootstrapper(), args);
        if (status != 0) {
            System.exit(status);
        }
    }

    /**
     * Used by the windows service wrapper
     */
    @SuppressWarnings("unused")
    public static void start(String[] args) {
        bootstrapper = new BlockingBootstrapper(new CommunityBootstrapper());
        System.exit(NeoBootstrapper.start(bootstrapper, args));
    }

    /**
     * Used by the windows service wrapper
     */
    @SuppressWarnings("unused")
    public static void stop(@SuppressWarnings("UnusedParameters") String[] args) {
        if (bootstrapper != null) {
            bootstrapper.stop();
        }
    }

    @Override
    public int getPriority() {
        return Priority.LOW.ordinal();
    }
}
