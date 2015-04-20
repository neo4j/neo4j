/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.desktop.config.unix;

import java.io.File;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.portable.PortableInstallation;

public class UnixInstallation extends PortableInstallation
{
    @Override
    public Environment getEnvironment()
    {
        return new UnixEnvironment();
    }

    @Override
    public File getConfigurationDirectory()
    {
        // On UNIX derived systems it makes sense to put the configurations in the parent directory of
        // the default.graphdb directory
        File databaseDirectory = getDatabaseDirectory();
        return databaseDirectory.getParentFile();
    }

    @Override
    public File getVmOptionsFile()
    {
        return new File( getConfigurationDirectory(), "." + NEO4J_VMOPTIONS_FILENAME );
    }

    @Override
    public File getServerConfigurationsFile()
    {
        return new File( getConfigurationDirectory(), "." + NEO4J_SERVER_PROPERTIES_FILENAME );
    }

    @Override
    protected File getDefaultDirectory()
    {
        return new File( System.getProperty( "user.home" ) );
    }
}
