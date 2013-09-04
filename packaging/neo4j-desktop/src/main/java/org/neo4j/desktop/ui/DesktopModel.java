/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.ui;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.neo4j.desktop.config.DatabaseConfiguration;
import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.Value;
import org.neo4j.kernel.Version;

import static java.lang.String.format;

public class DesktopModel
{
    private final Environment environment;
    private File databaseDirectory;
    private final Value<List<String>> extensionPackagesConfig;

    public DesktopModel( Environment environment, File databaseDirectory, Value<List<String>> extensionPackagesConfig )
    {
        this.environment = environment;
        this.databaseDirectory = databaseDirectory;
        this.extensionPackagesConfig = extensionPackagesConfig;
    }

    public String getNeo4jVersion()
    {
        return format( "%s", Version.getKernel().getReleaseVersion() );
    }

    public File getDatabaseDirectory()
    {
        return databaseDirectory;
    }

    public void setDatabaseDirectory( File databaseDirectory ) throws UnsuitableGraphDatabaseDirectory
    {
        verifyGraphDirectory( databaseDirectory );
        this.databaseDirectory = databaseDirectory;
    }

    public File getVmOptionsFile()
    {
        return new File( environment.getBaseDirectory(), "neo4j-community.vmoptions" );
    }

    public List<String> getExtensionPackagesConfig()
    {
        return extensionPackagesConfig.get();
    }

    public void setExtensionPackagesConfig( List<String> value )
    {
        extensionPackagesConfig.set( value );
    }

    public File getDatabaseConfigurationFile()
    {
        return new File( databaseDirectory, "neo4j.properties" );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void prepareGraphDirectoryForStart() throws UnsuitableGraphDatabaseDirectory, IOException
    {
        verifyGraphDirectory( databaseDirectory );
        if ( !databaseDirectory.exists() )
        {
            databaseDirectory.mkdirs();
        }

        File configurationFile = getDatabaseConfigurationFile();
        if ( !configurationFile.exists() )
        {
            DatabaseConfiguration.copyDefaultDatabaseConfigurationProperties( configurationFile );
        }
    }

    public void verifyGraphDirectory( File dir ) throws UnsuitableGraphDatabaseDirectory
    {
        if ( !dir.isDirectory() )
        {
            throw new UnsuitableGraphDatabaseDirectory( "%s is not a directory", dir );
        }

        if ( !dir.canWrite() )
        {
            throw new UnsuitableGraphDatabaseDirectory( "%s is not writeable", dir );
        }

        String[] fileNames = dir.list( new FilenameFilter()
        {
            @Override public boolean accept( File dir, String name )
            {
                return ! name.startsWith( "." );
            }
        } );

        if ( 0 == fileNames.length )
        {
            return;
        }

        for ( String fileName : fileNames )
        {
            if ( fileName.startsWith( "neostore" ) || fileName.equals( "neo4j.properties" ) )
            {
                return;
            }
        }

        throw new UnsuitableGraphDatabaseDirectory(
                "%s is neither empty nor does it contain a neo4j graph database", dir );
    }
}
