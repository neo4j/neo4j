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
package org.neo4j.shell;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import org.neo4j.shell.cli.Encryption;

public record ConnectionConfig(
        URI uri,
        String username,
        String password,
        Encryption encryption,
        String database,
        Optional<String> impersonatedUser) {

    public ConnectionConfig withPassword(String password) {
        return new ConnectionConfig(uri, username, password, encryption, database, impersonatedUser);
    }

    public ConnectionConfig withUsernameAndPassword(String username, String password) {
        return new ConnectionConfig(uri, username, password, encryption, database, impersonatedUser);
    }

    public ConnectionConfig withUsernameAndPasswordAndDatabase(String username, String password, String database) {
        return new ConnectionConfig(uri, username, password, encryption, database, impersonatedUser);
    }

    public ConnectionConfig withScheme(String scheme) {
        try {
            var newUri = new URI(
                    scheme,
                    uri.getUserInfo(),
                    uri.getHost(),
                    uri.getPort(),
                    uri.getPath(),
                    uri.getQuery(),
                    uri.getFragment());
            return new ConnectionConfig(newUri, username, password, encryption, database, impersonatedUser);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid scheme " + scheme, e);
        }
    }

    public ConnectionConfig withImpersonatedUser(String impersonatedUser) {
        return new ConnectionConfig(
                uri, username, password, encryption, database, Optional.ofNullable(impersonatedUser));
    }
}
