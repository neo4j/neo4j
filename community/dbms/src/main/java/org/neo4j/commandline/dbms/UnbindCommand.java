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

import static picocli.CommandLine.Command;

import java.io.IOException;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;

@Command(
        name = "unbind",
        header = "Removes server identifier.",
        description = "Removes server identifier. Next start instance will create a new identity for itself.")
public class UnbindCommand extends AbstractAdminCommand {
    public UnbindCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    public void execute() {
        try {
            var config = buildConfig();
            var neo4jLayout = Neo4jLayout.of(config);
            try (var ignored = LockChecker.checkDbmsLock(neo4jLayout)) {
                execute(config, neo4jLayout);
            }
        } catch (FileLockException e) {
            throw new CommandFailedException("Database is currently locked. Please shutdown database.", e);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected void execute(Config config, Neo4jLayout neo4jLayout) throws IOException {
        ctx.fs().deleteFile(neo4jLayout.serverIdFile());
    }

    private Config buildConfig() {
        return createPrefilledConfigBuilder().build();
    }
}
