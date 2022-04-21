/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.commandline.admin.security;

import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.string.UTF8;
import org.neo4j.util.VisibleForTesting;

@Command(
        name = "set-initial-password",
        description =
                "Sets the initial password of the initial admin user ('" + INITIAL_USER_NAME + "'). "
                        + "And removes the requirement to change password on first login. "
                        + "IMPORTANT: this change will only take effect if performed before the database is started for the first time.")
public class SetInitialPasswordCommand extends AbstractCommand implements PasswordCommand {
    @Option(
            names = "--require-password-change",
            defaultValue = "false",
            description = "Require the user to change their password on first login.")
    private boolean changeRequired;

    @Parameters
    private String password;

    public SetInitialPasswordCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    public void execute() throws IOException {
        Config config = loadNeo4jConfig();
        FileSystemAbstraction fileSystem = ctx.fs();

        Path file = CommunitySecurityModule.getInitialUserRepositoryFile(config);
        if (fileSystem.fileExists(file)) {
            fileSystem.deleteFile(file);
        }

        FileUserRepository userRepository = new FileUserRepository(fileSystem, file, NullLogProvider.getInstance());
        try {
            userRepository.start();
            userRepository.create(
                    new User.Builder(INITIAL_USER_NAME, createCredentialForPassword(UTF8.encode(password)))
                            .withRequiredPasswordChange(changeRequired)
                            .build());
            userRepository.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ctx.out()
                .println(
                        "Changed password for user '" + INITIAL_USER_NAME + "'. "
                                + "IMPORTANT: this change will only take effect if performed before the database is started for the first time.");
    }

    @VisibleForTesting
    Config loadNeo4jConfig() {
        Config cfg = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, ctx.homeDir().toAbsolutePath())
                .fromFileNoThrow(ctx.confDir().resolve(Config.DEFAULT_CONFIG_FILE_NAME))
                .commandExpansion(allowCommandExpansion)
                .build();
        ConfigUtils.disableAllConnectors(cfg);
        return cfg;
    }
}
