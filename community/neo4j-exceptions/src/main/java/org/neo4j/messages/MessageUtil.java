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

import java.util.Set;
import java.util.TreeSet;

/**
 * Collection of error and log messages (usable in product and test classes)
 */
public class MessageUtil
{
    // autentication
    private static final String CREATE_NODE_WITH_LABELS_DENIED = "Create node with labels '%s' on database '%s' is not allowed for %s.";
    private static final String WITH_USER = "user '%s' with %s";
    private static final String OVERRIDEN_MODE = "%s overridden by %s";
    private static final String RESTRICTED_MODE = "%s restricted to %s";

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
    public static String overridenMode( String original, String wrapping )
    {
        return String.format(OVERRIDEN_MODE, original, wrapping);
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
}
