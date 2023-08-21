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

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "windows-service",
        description = "Neo4j windows service commands.",
        subcommands = {
            WindowsServiceCommand.Install.class,
            WindowsServiceCommand.Update.class,
            WindowsServiceCommand.Uninstall.class,
            CommandLine.HelpCommand.class
        })
public class WindowsServiceCommand {

    @CommandLine.Option(
            names = {"-h", "--help"},
            usageHelp = true,
            fallbackValue = "true",
            description = "Show this help message and exit.")
    private boolean helpRequested;

    @Command(name = "install", description = "Install the Windows service.")
    public static class Install extends AbstractCommand {

        public Install(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        protected void execute() throws Exception {
            var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
            try (var bootloader = enhancedCtx.createDbmsBootloader()) {
                bootloader.installService();
            }
        }
    }

    @Command(name = "uninstall", description = "Uninstall the Windows service.")
    public static class Uninstall extends AbstractCommand {

        public Uninstall(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        protected void execute() throws Exception {
            var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
            try (var bootloader = enhancedCtx.createDbmsBootloader()) {
                bootloader.uninstallService();
            }
        }
    }

    @Command(name = "update", description = "Update the Windows service.")
    public static class Update extends AbstractCommand {

        public Update(ExecutionContext ctx) {
            super(ctx);
        }

        @Override
        protected void execute() throws Exception {
            var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
            try (var bootloader = enhancedCtx.createDbmsBootloader()) {
                bootloader.updateService();
            }
        }
    }
}
