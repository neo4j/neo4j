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
package org.neo4j.desktop.config.portable;

import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.desktop.config.Installation;

import static java.lang.String.format;

public abstract class PortableInstallation implements Installation
{

    protected void mkdirs( File path, String description )
    {
        if ( path.exists() )
        {
            if ( !path.isDirectory() )
            {
                throw new PathAlreadyExistException( path, description );
            }
        }
        else if ( !path.mkdirs() )
        {
            throw new CannotMakeDirectory( path, description );
        }
    }

    @Override
    public File getPluginsDirectory()
    {
        try
        {
            File installationDirectory = getInstallationDirectory();
            return new File( installationDirectory, "plugins" );
        }
        catch ( URISyntaxException e )
        {
            throw new CannotFindInstallationDirectory( e );
        }
    }

    @Override
    public File getInstallationDirectory() throws URISyntaxException
    {
        return getInstallationBinDirectory().getParentFile();
    }

    @Override
    public File getInstallationBinDirectory() throws URISyntaxException
    {
        File appFile = new File( Installation.class.getProtectionDomain().getCodeSource().getLocation().toURI() );
        return appFile.getParentFile();
    }

    @Override
    public File getInstallationJreBinDirectory() throws URISyntaxException
    {
        return new File( getInstallationDirectory(), "jre/bin" );
    }

    private static class PathAlreadyExistException extends RuntimeException
    {
        public PathAlreadyExistException( File path, String description )
        {
            super( format( "%s already exists but is not a %s.", description, path.getAbsolutePath() ) );
        }
    }

    private static class CannotMakeDirectory extends RuntimeException
    {
        public CannotMakeDirectory( File path, String description )
        {
            super( format( "Could not make %s %s", description, path.getAbsolutePath() ) );
        }
    }

    private static class CannotFindInstallationDirectory extends RuntimeException
    {
        public CannotFindInstallationDirectory( Exception cause )
        {
            super( cause );
        }
    }

    @Override
    public File getDatabaseDirectory()
    {
        List<File> locations = new ArrayList<>();

        File defaultDirectory = getDefaultDirectory();
        File userHome = new File( System.getProperty( "user.home" ) );
        File userDir = new File( System.getProperty( "user.dir" ) );

        locations.add( defaultDirectory );
        locations.add( userHome );

        File documents = selectFirstWritableDirectoryOrElse( locations, userDir );
        File neo4jData = new File( documents, "Neo4j" );
        File graphdb = new File( neo4jData, "default.graphdb" );

        mkdirs( graphdb, "Neo4j data directory" );

        return graphdb;
    }

    protected abstract File getDefaultDirectory();

    private static File selectFirstWritableDirectoryOrElse( List<File> locations, File defaultFile )
    {
        File result = defaultFile.getAbsoluteFile();
        for ( File file : locations )
        {
            File candidateFile = file.getAbsoluteFile();
            if ( candidateFile.exists() && candidateFile.isDirectory() && candidateFile.canWrite() ) {
                result = candidateFile;
                break;
            }
        }
        return result;
    }

    @Override
    public File getDatabaseConfigurationFile()
    {
        return new File( getDatabaseDirectory(), NEO4J_PROPERTIES_FILENAME );
    }

    @Override
    public void initialize() throws Exception
    {
        File vmopts = getVmOptionsFile();

        if ( !vmopts.exists() )
        {
            createVmOptionsFile( vmopts );
        }
    }

    private void createVmOptionsFile( File file ) throws Exception
    {
        Template template = new Template( getDefaultVmOptions() );
        template.write( file );
    }

    @Override
    public InputStream getDefaultDatabaseConfiguration()
    {
        return getResourceStream( DEFAULT_DATABASE_CONFIG_RESOURCE_NAME );
    }

    @Override
    public InputStream getDefaultServerConfiguration()
    {
        return getResourceStream( DEFAULT_SERVER_CONFIG_RESOURCE_NAME );
    }

    @Override
    public InputStream getDefaultVmOptions()
    {
        return getResourceStream( DEFAULT_VMOPTIONS_TEMPLATE_RESOURCE_NAME );
    }

    private InputStream getResourceStream( String defaultDatabaseConfigResourceName )
    {
        return PortableInstallation.class.getResourceAsStream( defaultDatabaseConfigResourceName );
    }
}
