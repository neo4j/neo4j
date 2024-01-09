/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cloud.storage;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.regex.Pattern;

public interface StorageSchemeResolver {

    // RFC2396: alpha *( alpha | digit | "+" | "-" | "." )
    Pattern SCHEME = Pattern.compile("^([a-z][a-z0-9+-.]+):.+");

    /**
     * Determine if a resource path is an absolute {@link URI} with a scheme. See RFC2396 for more details
     * @param resource the resource path to check
     * @return <code>true</code> if the resource provided is a scheme based {@link URI}
     */
    static boolean isSchemeBased(String resource) {
        return SCHEME.matcher(resource).matches();
    }

    /**
     * Determine if the resource {@link URI} can be resolved by this system into an implementation-dependent {@link Path}
     * @param resource the resource path to check
     * @return <code>true</code> if the resource provided can be resolved by the system
     */
    boolean canResolve(URI resource);

    /**
     * Determine if the resource path can be resolved by this system into an implementation-dependent
     * {@link Path}
     * @param resource the resource path to check
     * @return <code>true</code> if the resource provided can be resolved by the system
     */
    boolean canResolve(String resource);

    /**
     * Resolve a resource into an implementation-dependent {@link Path} object
     * @param resource the resource to resolve
     * @return the resolved path
     * @throws IOException if unable resolve the provided resource
     */
    Path resolve(URI resource) throws IOException;

    /**
     * Resolve a resource into an implementation-dependent {@link Path} object
     * @param resource the resource to resolve
     * @return the resolved path
     * @throws IOException if unable resolve the provided resource
     */
    Path resolve(String resource) throws IOException;
}
