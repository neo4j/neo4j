/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.config.windows;

import java.io.File;
import java.io.IOException;

import org.neo4j.desktop.config.portable.Environment;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.join;

class WindowsEnvironment extends Environment
{

    @Override
    public void editFile( File file ) throws IOException, SecurityException
    {
        try
        {
            super.editFile( file );
        }
        catch ( IOException | UnsupportedOperationException ex )
        {
            getRuntime().exec( new String[]{"notepad", file.getAbsolutePath()} );
        }
    }

    @Override
    public void openDirectory( File directory ) throws IOException
    {
        try
        {
            super.openDirectory( directory );
        }
        catch ( IOException | UnsupportedOperationException ex )
        {
            getRuntime().exec( new String[]{"explorer", directory.getAbsolutePath()} );
        }
    }

    @Override
    public void openCommandPrompt( File binDirectory, File jreBinDirectory, File workingDirectory ) throws IOException
    {
        String[] shellStartupCommands = {
                format( "set PATH=\"%s\";\"%s\";%%PATH%%", jreBinDirectory, binDirectory ),
                "set REPO=" + binDirectory,
                "cd /D " + workingDirectory,
                "echo Neo4j Command Prompt",
                "echo.",
                "echo This window is configured with Neo4j on the path.",
                "echo.",
                "echo Available commands:",
                "echo * Neo4jShell",
                "echo * Neo4jImport"
        };

        String[] cmdArray = {
                "cmd",
                "/C",
                format( "start \"Neo4j Command Prompt\" cmd /K \"%s\"", join( shellStartupCommands, " && " ) )
        };

        getRuntime().exec( cmdArray );
    }
}
