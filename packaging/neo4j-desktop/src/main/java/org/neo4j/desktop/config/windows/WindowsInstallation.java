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
import java.io.FileInputStream;
import java.util.Properties;

import org.neo4j.desktop.config.portable.Environment;
import org.neo4j.desktop.config.portable.PortableInstallation;

import static javax.swing.filechooser.FileSystemView.getFileSystemView;

public class WindowsInstallation extends PortableInstallation
{
    private final WindowsEnvironment environment;
    private final Properties config = new Properties();

    public WindowsInstallation() throws Exception
    {
        environment = new WindowsEnvironment();
        config.load( new FileInputStream( new File( getInstallationBinDirectory(), INSTALL_PROPERTIES_FILENAME ) ) );
    }

    @Override
    public Environment getEnvironment()
    {
        return environment;
    }

    @Override
    protected File getDefaultDirectory()
    {
        return getFileSystemView().getDefaultDirectory();
    }

    @Override
    public File getConfigurationDirectory()
    {
        File appData = new File( System.getenv( "APPDATA" ) );
        return new File( appData, config.getProperty( "win.appdata.subdir" ) );
    }

    @Override
    public File getVmOptionsFile()
    {
        return new File( getConfigurationDirectory(), NEO4J_VMOPTIONS_FILENAME );
    }

    @Override
    public File getConfigurationsFile()
    {
        return new File( getConfigurationDirectory(), NEO4J_CONFIG_FILENAME );
    }
}
