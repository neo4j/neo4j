/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import static java.lang.System.getProperty;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.commons.lang3.StringUtils.defaultString;

public class Version
{
    static final String CUSTOM_VERSION_SETTING = "unsupported.neo4j.custom.version";
    private static final String DEFAULT_DEV_VERSION = "dev";
    private static final String KERNEL_ARTIFACT_ID = "neo4j-kernel";
    private static final Version KERNEL_VERSION = new Version( KERNEL_ARTIFACT_ID, selectVersion() );

    static String selectVersion()
    {
        var versionString = getProperty( CUSTOM_VERSION_SETTING, Version.class.getPackage().getImplementationVersion() );
        return defaultString( versionString, DEFAULT_DEV_VERSION );
    }

    private final String artifactId;
    private final String title;
    private final String version;
    private final String releaseVersion;

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
        else
        {
            result.append( requireNonNullElse( artifactId, "Unknown Component" ) );
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
        requireNonNull( artifactId );
        requireNonNull( version );
        this.artifactId = artifactId;
        this.title = artifactId;
        this.version = version;
        this.releaseVersion = parseReleaseVersion( this.version );
    }

    /**
     * This reads out the user friendly part of the version, for public display.
     */
    private static String parseReleaseVersion( String fullVersion )
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
        return fullVersion;
    }
}
