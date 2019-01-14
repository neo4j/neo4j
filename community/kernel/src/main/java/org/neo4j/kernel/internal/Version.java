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
package org.neo4j.kernel.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.helpers.Service;

public class Version extends Service
{
    public static Version getKernel()
    {
        return KERNEL_VERSION;
    }

    public static String getKernelVersion()
    {
        return getKernel().getVersion();
    }

    public static String getNeo4jVersion()
    {
        return getKernel().getReleaseVersion();
    }

    private final String artifactId;
    private final String title;
    private final String vendor;
    private final String version;
    private final String releaseVersion;

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        if ( title != null )
        {
            result.append( title );
            if ( artifactId == null || !artifactId.equals( title ) )
            {
                result.append( " (" ).append( artifactId ).append( ')' );
            }
        }
        else if ( artifactId != null )
        {
            result.append( artifactId );
        }
        else
        {
            result.append( "Unknown Component" );
        }
        result.append( ", " );
        if ( title == null )
        {
            result.append( "unpackaged " );
        }
        result.append( "version: " ).append( getVersion() );
        return result.toString();
    }

    /**
     * @return a detailed version string, including source control revision information if that is available, suitable
     * for internal use, logging and debugging.
     */
    public final String getVersion()
    {
        return version;
    }

    /**
     * @return a user-friendly version string, like "1.0.0-M01" or "2.0.0", suitable for end-user display
     */
    public String getReleaseVersion()
    {
        return releaseVersion;
    }

    protected Version( String artifactId, String version )
    {
        super( artifactId );
        this.artifactId = artifactId;
        this.title = artifactId;
        this.vendor = "Neo Technology";
        this.version = version == null ? "dev" : version;
        this.releaseVersion = parseReleaseVersion( this.version );
    }

    /**
     * This reads out the user friendly part of the version, for public display.
     */
    private String parseReleaseVersion( String fullVersion )
    {
        // Generally, a version we extract from the jar manifest will look like:
        //   1.2.3-M01,abcdef-dirty
        // Parse out the first part of it:
        Pattern pattern = Pattern.compile(
                "(\\d+" +                  // Major version
                "\\.\\d+" +                // Minor version
                "(\\.\\d+)?" +             // Optional patch version
                "(-?[^,]+)?)" +          // Optional marker, like M01, GA, SNAPSHOT - anything other than a comma
                ".*"                       // Anything else, such as git revision
        );

        Matcher matcher = pattern.matcher( fullVersion );
        if ( matcher.matches() )
        {
            return matcher.group( 1 );
        }

        // If we don't recognize the version pattern, do the safe thing and keep it in full
        return version;
    }

    /**
     * A very nice to have main-method for quickly checking the version of a neo4j kernel,
     * for example given a kernel jar file.
     */
    public static void main( String[] args )
    {
        Version kernelVersion = getKernel();
        System.out.println( kernelVersion );
        System.out.println( "Title: " + kernelVersion.title );
        System.out.println( "Vendor: " + kernelVersion.vendor );
        System.out.println( "ArtifactId: " + kernelVersion.artifactId );
        System.out.println( "Version: " + kernelVersion.getVersion() );
    }

    static final String KERNEL_ARTIFACT_ID = "neo4j-kernel";
    private static final Version KERNEL_VERSION = new Version( KERNEL_ARTIFACT_ID,
            Version.class.getPackage().getImplementationVersion() );
}
