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
package org.neo4j.desktop.config.osx;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;

import org.neo4j.desktop.config.portable.Environment;
import org.neo4j.desktop.config.unix.UnixInstallation;

public class DarwinInstallation extends UnixInstallation
{

    public DarwinInstallation()
    {
        String filename = "openNeoTerminal.sh";

        try
        {
            String[] scriptCommands = {
                    "#!/bin/bash",
                    "export PATH=$PATH:'" + this.getInstallationBinDirectory().getAbsolutePath().toString() + "':'" +
                    this.getInstallationJreBinDirectory().getAbsolutePath().toString() + "'",
                    "echo Neo4j Command Prompt",
                    "echo",
                    "echo This window is configured with Neo4j on the path.",
                    "echo",
                    "echo Available commands:",
                    "echo neo4j-shell",
                    "echo neo4j-import",
                    "bash"};

            try ( FileWriter fileWriter = new FileWriter( new File( filename ), false ) )
            {
                for ( String scriptCommand : scriptCommands )
                {
                    fileWriter.write( scriptCommand + "\n" );
                }

                String[] commands = {"bash", "-c", "chmod a+x " + filename};

                Runtime.getRuntime().exec( commands );
            }
            catch ( IOException ioe )
            {
                System.out.println( "Error writing " + filename );
            }
        }
        catch ( URISyntaxException urise )
        {
            System.out.println( "Error getting bin locations for " + filename );
        }
    }

    public Environment getEnvironment()
    {
        return new DarwinEnvironment();
    }

    @Override
    protected File getDefaultDirectory()
    {
        return new File( new File( System.getProperty( "user.home" ) ), "Documents" );
    }
}
