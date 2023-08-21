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
package org.neo4j.commandline.dbms;

import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.server.startup.EnhancedExecutionContext;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = "migrate-configuration",
        header = "Migrate server configuration from the previous major version.",
        description = "Migrate legacy configuration located in source configuration directory to the current format. "
                + "The new version will be written in a target configuration directory. "
                + "The default location for both the source and target configuration directory "
                + "is the configuration directory specified by NEO_CONF or the default configuration "
                + "directory for this installation. "
                + "If the source and target directories are the same, the original configuration files will "
                + "be renamed. "
                + "Configuration provided using --additional-config option is not migrated.")
public class MigrateConfigCommand extends AbstractCommand {

    @Option(
            names = "--from-path",
            paramLabel = "<path>",
            description = "Path to the configuration directory used as a source for the migration.")
    private Path fromPath;

    @Option(
            names = "--to-path",
            paramLabel = "<path>",
            description = "Path to a directory where the migrated configuration files should be written.")
    private Path toPath;

    public MigrateConfigCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected void execute() throws Exception {
        Path sourceFile = configFile(fromPath);
        if (!Files.isRegularFile(sourceFile)) {
            throw new CommandFailedException(String.format("Resolved source file '%s' does not exist", sourceFile));
        }
        Path targetFile = configFile(toPath);
        if (!Files.isDirectory(targetFile.getParent())) {
            throw new CommandFailedException(
                    String.format("Target path '%s' is not an existing directory", targetFile.getParent()));
        }
        var enhancedCtx = EnhancedExecutionContext.unwrapFromExecutionContext(ctx);
        var migrator = new ConfigFileMigrator(ctx.out(), ctx.err(), enhancedCtx.getClassloaderWithPlugins());
        migrator.migrate(sourceFile, targetFile);
    }

    private Path configFile(Path providedConfigDir) {
        if (providedConfigDir == null) {
            providedConfigDir = ctx.confDir();
        } else {
            if (!Files.isDirectory(providedConfigDir)) {
                throw new CommandFailedException(
                        String.format("Provided path '%s' is not an existing directory", providedConfigDir));
            }
        }

        return providedConfigDir.resolve(Config.DEFAULT_CONFIG_FILE_NAME);
    }
}
