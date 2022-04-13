/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.messages;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Collection of error and log messages (usable in product and test classes)
 */
public class MessageUtil
{
    // authentication
    private static final String CREATE_NODE_WITH_LABELS_DENIED = "Create node with labels '%s' on database '%s' is not allowed for %s.";
    private static final String WITH_USER = "user '%s' with %s";
    private static final String OVERRIDDEN_MODE = "%s overridden by %s";
    private static final String RESTRICTED_MODE = "%s restricted to %s";

    // alias
    private static final String ALTER_TO_REMOTE = "Failed to alter the specified database alias '%s': alter a local alias to a remote alias is not supported.";
    private static final String FAILED_TO_READ_REMOTE_ALIAS_KEY = "Failed to read the symmetric key from the configured keystore";
    /**
     * authentication & authorization messages
     */
    public static String createNodeWithLabelsDenied( String labels, String database, String user )
    {
        return String.format( CREATE_NODE_WITH_LABELS_DENIED, labels, database, user );
    }

    // security context
    public static String authDisabled( String mode )
    {
        return "AUTH_DISABLED with " + mode;
    }

    // username description
    public static String withUser( String user, String mode )
    {
        return String.format( WITH_USER, user, mode );
    }

    // mode names
    public static String overriddenMode( String original, String wrapping )
    {
        return String.format( OVERRIDDEN_MODE, original, wrapping);
    }

    public static String restrictedMode( String original, String wrapping )
    {
        return String.format( RESTRICTED_MODE, original, wrapping );
    }

    public static String standardMode( Set<String> roles )
    {
        Set<String> sortedRoles = new TreeSet<>( roles );
        return roles.isEmpty() ? "no roles" : "roles " + sortedRoles;
    }

    /**
     * alias messages
     */
    public static String alterToLocalAlias( String alias )
    {
        return String.format( ALTER_TO_REMOTE, alias );
    }

    public static String failedToFindEncryptionKeyInKeystore( String keyName )
    {
        return String.format( "%s. The key '%s' was not found in the given keystore file.",
                              FAILED_TO_READ_REMOTE_ALIAS_KEY, keyName );
    }

    public static String failedToReadRemoteAliasEncryptionKey( String... settings )
    {
        return String.format( "%s. Please verify the keystore configurations: %s.",
                              FAILED_TO_READ_REMOTE_ALIAS_KEY, StringUtils.join( settings, ", " ) );
    }

    public static String failedToEncryptPassword()
    {
        return "Failed to encrypt remote user password.";
    }

    public static String failedToDecryptPassword()
    {
        return "Failed to decrypt remote user password.";
    }

    public static String invalidScheme( String url, List<String> schemes )
    {
        return String.format( "The provided url '%s' has an invalid scheme. Please use one of the following schemes: %s.", url,
                              StringUtils.join( schemes, ", " ) );
    }

    public static String insecureScheme( String url, List<String> schemes )
    {
        return String.format( "The provided url '%s' is not a secure scheme. Please use one of the following schemes: %s.", url,
                              StringUtils.join( schemes, ", " ) );
    }
}
