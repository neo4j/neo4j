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
package org.neo4j.shell;

import java.util.Optional;
import org.neo4j.shell.cli.Encryption;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record ConnectionConfig(
        String scheme,
        String host,
        int port,
        String username,
        String password,
        Encryption encryption,
        String database,
        Environment environment,
        Optional<String> impersonatedUser) {
    public static final String USERNAME_ENV_VAR = "NEO4J_USERNAME";
    public static final String PASSWORD_ENV_VAR = "NEO4J_PASSWORD";
    public static final String DATABASE_ENV_VAR = "NEO4J_DATABASE";

    public static ConnectionConfig connectionConfig(
            String scheme,
            String host,
            int port,
            String username,
            String password,
            Encryption encryption,
            String database,
            Environment environment,
            Optional<String> impersonatedUser) {
        return new ConnectionConfig(
                scheme,
                host,
                port,
                fallbackToEnvVariable(environment, username, USERNAME_ENV_VAR),
                fallbackToEnvVariable(environment, password, PASSWORD_ENV_VAR),
                encryption,
                fallbackToEnvVariable(environment, database, DATABASE_ENV_VAR),
                environment,
                impersonatedUser);
    }

    public static ConnectionConfig connectionConfig(
            String scheme,
            String host,
            int port,
            String username,
            String password,
            Encryption encryption,
            String database,
            Environment environment) {
        return new ConnectionConfig(
                scheme,
                host,
                port,
                fallbackToEnvVariable(environment, username, USERNAME_ENV_VAR),
                fallbackToEnvVariable(environment, password, PASSWORD_ENV_VAR),
                encryption,
                fallbackToEnvVariable(environment, database, DATABASE_ENV_VAR),
                environment,
                Optional.empty());
    }

    /**
     * @return preferredValue if not empty, else the contents of the fallback environment variable
     */
    private static String fallbackToEnvVariable(Environment environment, String preferredValue, String fallbackEnvVar) {
        String result = environment.getVariable(fallbackEnvVar);
        if (result == null || !preferredValue.isEmpty()) {
            result = preferredValue;
        }
        return result;
    }

    public String driverUrl() {
        return String.format("%s://%s:%d", scheme(), host(), port());
    }

    public ConnectionConfig withPassword(String password) {
        return new ConnectionConfig(
                scheme, host, port, username, password, encryption, database, environment, impersonatedUser);
    }

    public ConnectionConfig withUsernameAndPassword(String username, String password) {
        return new ConnectionConfig(
                scheme, host, port, username, password, encryption, database, environment, impersonatedUser);
    }

    public ConnectionConfig withUsernameAndPasswordAndDatabase(String username, String password, String database) {
        return new ConnectionConfig(
                scheme,
                host,
                port,
                username,
                password,
                encryption,
                fallbackToEnvVariable(environment, database, DATABASE_ENV_VAR),
                environment,
                impersonatedUser);
    }

    public ConnectionConfig withScheme(String scheme) {
        return new ConnectionConfig(
                scheme, host, port, username, password, encryption, database, environment, impersonatedUser);
    }

    public ConnectionConfig withImpersonatedUser(String impersonatedUser) {
        return new ConnectionConfig(
                scheme,
                host,
                port,
                username,
                password,
                encryption,
                database,
                environment,
                Optional.ofNullable(impersonatedUser));
    }
}
