/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.release.it.std.exec;

import org.ops4j.pax.runner.platform.internal.StreamUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 */
public class Provisioner
{
    private String provisionDirectoryName = ".";
    private Set<ArtifactProvision> artifacts = new HashSet<ArtifactProvision>();
    private Set<String> classpathDirectories = new HashSet<String>();

    static
    {
        System.setProperty( "java.protocol.handler.pkgs", "org.ops4j.pax.url" );
    }

    public Provisioner()
    {

    }

    public void setProvisionDirectory( String provisionDirectory )
    {
        this.provisionDirectoryName = provisionDirectory;
    }

    public void provision()
    {
        File provisionDirectory = new File( provisionDirectoryName );
        provisionDirectory.mkdir();

        for ( ArtifactProvision artifact : artifacts )
        {
            try
            {
                BufferedOutputStream bos = new BufferedOutputStream( new FileOutputStream( new File( provisionDirectory, artifact.getDestinationFilename() ) ) );
                StreamUtils.streamCopy( artifact.getSourceURL(), bos, null );
            } catch ( IOException ioe )
            {
                System.err.println( "Failed to provision " + artifact + ", because: " + ioe );
            }
        }
    }

    public void include( String artifactAtURL, String asFilename ) throws MalformedURLException
    {
        include( new URL( artifactAtURL ), asFilename );
    }

    public void include( URL artifactAtURL, String asFilename )
    {
        include( new ArtifactProvision( artifactAtURL, asFilename ) );
    }

    public void include( ArtifactProvision artifact )
    {
        this.artifacts.add( artifact );
    }

    public String[] getProvisionedClasspath()
    {
        File basedir = new File( provisionDirectoryName );

        ArrayList<String> classpath = new ArrayList<String>();

        for ( ArtifactProvision artifact : artifacts )
        {
            classpath.add( new File( basedir, artifact.getDestinationFilename() ).getAbsolutePath() );
        }


        for ( String classpathDirectory : classpathDirectories )
        {
            classpath.add( classpathDirectory );
        }

        return classpath.toArray( new String[classpath.size()] );
    }

    public void includeTargetClasses()
    {
        String currentClasspath = System.getProperty( "java.class.path" );
        String[] currentParts = currentClasspath.split( ":" );
        for ( String part : currentParts )
        {
            if ( part.contains( "target" + File.separator + "classes" ) )
            {
                this.classpathDirectories.add( part );
            }
        }
    }

    public void includeTargetTestClasses()
    {
        String currentClasspath = System.getProperty( "java.class.path" );
        String[] currentParts = currentClasspath.split( ":" );
        for ( String part : currentParts )
        {
            if ( part.contains( "target" + File.separator + "test-classes" ) )
            {
                this.classpathDirectories.add( part );
            }
        }
    }
}
