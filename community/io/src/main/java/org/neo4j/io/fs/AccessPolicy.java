/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.io.fs;

/**
 * This defines file access policies. The policies describe in prose
 * what the policy should do.
 * <p>
 * While there are some older systems that have no support for file permissions,
 * in practice there are three major security models in wide use:
 * <p>
 * - POSIX File Permissions (OS X, Linux, Unixes)
 * - Windows NT-style ACLs
 * - POSIX.1e ACLs (Modern Linux File Systems, Solaris..)
 * <p>
 * This design is intended to convey things like "someone able to read this file can compromise
 * the dbms", "this file should be writable by end users that want to communicate with neo"
 * and so on; leaving it up to platform-specific implementations to then apply appropriate
 * access rules.
 */
public enum AccessPolicy
{
    /**
     * Ability to read or write to this file will compromise the dbms and the data in it.
     * Files with this policy should only be accessible by users that are meant to have
     * unrestricted access to the system.
     */
    CRITICAL

    // Please add additional policies as needed!
}
