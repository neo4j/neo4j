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
package org.neo4j.commandline.admin.security;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;

import java.nio.file.Path;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.util.VisibleForTesting;

@Command(
        name = "set-default-admin",
        description = "Sets the default admin user.%n"
                + "This user will be granted the admin role on startup if the system has no roles.")
public class SetDefaultAdminCommand extends AbstractAdminCommand {
    public static final String ADMIN_INI = "admin.ini";

    @Parameters
    private String username;

    public SetDefaultAdminCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    public void execute() {
        Config config = loadNeo4jConfig();
        try {
            Path adminIniFile =
                    CommunitySecurityModule.getInitialUserRepositoryFile(config).resolveSibling(ADMIN_INI);
            FileSystemAbstraction fs = ctx.fs();
            var memoryTracher = EmptyMemoryTracker.INSTANCE;
            if (fs.fileExists(adminIniFile)) {
                fs.deleteFile(adminIniFile);
            }
            UserRepository admins =
                    new FileUserRepository(fs, adminIniFile, NullLogProvider.getInstance(), memoryTracher);
            admins.init();
            admins.start();
            admins.create(new User(username, null, LegacyCredential.INACCESSIBLE, false, false));
            admins.stop();
            admins.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ctx.out().println("default admin user set to '" + username + "'");
    }

    @VisibleForTesting
    Config loadNeo4jConfig() {
        Config cfg = Config.newBuilder()
                .fromFileNoThrow(ctx.confDir().resolve(Config.DEFAULT_CONFIG_FILE_NAME))
                .set(GraphDatabaseSettings.neo4j_home, ctx.homeDir().toAbsolutePath())
                .commandExpansion(allowCommandExpansion)
                .build();
        ConfigUtils.disableAllConnectors(cfg);
        return cfg;
    }
}
