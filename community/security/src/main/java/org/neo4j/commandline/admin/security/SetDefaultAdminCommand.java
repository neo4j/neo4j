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
package org.neo4j.commandline.admin.security;

import java.io.File;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.CommunitySecurityModule;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.util.VisibleForTesting;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Parameters;

@Command(
        name = "set-default-admin",
        description = "Sets the default admin user.%n" +
                      "This user will be granted the admin role on startup if the system has no roles."
)
public class SetDefaultAdminCommand extends AbstractCommand
{
    public static final String ADMIN_INI = "admin.ini";

    @Parameters
    private String username;

    public SetDefaultAdminCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute()
    {
        Config config = loadNeo4jConfig();
        try
        {
            File adminIniFile = new File( CommunitySecurityModule.getUserRepositoryFile( config ).getParentFile(), ADMIN_INI );
            if ( ctx.fs().fileExists( adminIniFile ) )
            {
                ctx.fs().deleteFile( adminIniFile );
            }
            UserRepository admins = new FileUserRepository( ctx.fs(), adminIniFile, NullLogProvider.getInstance() );
            admins.init();
            admins.start();
            admins.create( new User.Builder( username, LegacyCredential.INACCESSIBLE ).build() );
            admins.stop();
            admins.shutdown();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }

        ctx.out().println( "default admin user set to '" + username + "'" );
    }

    @VisibleForTesting
    Config loadNeo4jConfig()
    {
        Config cfg = Config.newBuilder()
                .fromFileNoThrow( ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir().toAbsolutePath() ).build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }
}
