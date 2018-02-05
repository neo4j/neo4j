/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth.plugin.api;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Optional;

/**
 * These are the methods that the plugin can perform on Neo4j.
 */
public interface AuthProviderOperations
{
    /**
     * Returns the path to the Neo4j home directory.
     *
     * @return the path to the Neo4j home directory
     */
    Path neo4jHome();

    /**
     * Returns the path to the Neo4j configuration file if one exists.
     *
     * @return the path to the Neo4j configuration file if one exists
     *
     * @deprecated
     * Settings are recommended to be stored in a separate file. You can use {@link AuthProviderOperations#neo4jHome()}
     * to resolve your configuration file, e.g. {@code neo4jHome().resolve("conf/myPlugin.conf" );}
     */
    @Deprecated
    Optional<Path> neo4jConfigFile();

    /**
     * Returns the Neo4j version.
     *
     * @return the Neo4j version
     */
    String neo4jVersion();

    /**
     * Returns the clock that is used by the Neo4j security module within which this auth provider plugin is running.
     *
     * @return the clock that is used by the Neo4j security module
     */
    Clock clock();

    /**
     * Returns the security log that is used by the Neo4j security module within which this auth provider plugin is
     * running.
     *
     * @return the security log that is used by the Neo4j security module
     */
    Log log();

    /**
     * An interface to the security log that is used by the Neo4j security module.
     */
    interface Log
    {
        /**
         * Writes to the security log at log level debug.
         *
         * @param message the message to write to the security log
         */
        void debug( String message );

        /**
         * Writes to the security log at log level info.
         *
         * @param message the message to write to the security log
         */
        void info( String message );

        /**
         * Writes to the security log at log level warning.
         *
         * @param message the message to write to the security log
         */
        void warn( String message );

        /**
         * Writes to the security log at log level error.
         *
         * @param message the message to write to the security log
         */
        void error( String message );

        /**
         * Returns {@code true} if log level debug is enabled.
         *
         * @return {@code true} if log level debug is enabled, otherwise {@code false}
         */
        boolean isDebugEnabled();
    }

    /**
     * If set to {@code true} the authentication information returned by the plugin will be cached.
     * The expiration time of the cached information is configured by the
     * {@code dbms.security.auth_cache_ttl} configuration setting.
     *
     * <p>Since a principal can be authenticated against cached authentication information this requires
     * the capability of matching the credentials of an authentication token against the credentials of the
     * authentication information returned by the plugin.
     *
     * <p>The default value is {@code false}.
     *
     * @param authenticationCachingEnabled if caching of authentication information should be enabled or not
     */
    void setAuthenticationCachingEnabled( boolean authenticationCachingEnabled );

    /**
     * If set to {@code true} the authorization information (i.e. the list of roles for a given principal)
     * returned by the plugin will be cached.
     * The expiration time of the cached information is configured by the
     * {@code dbms.security.auth_cache_ttl} configuration setting.
     *
     * The default value is {@code true}.
     *
     * @param authorizationCachingEnabled if caching of authorization information should be enabled or not
     */
    void setAuthorizationCachingEnabled( boolean authorizationCachingEnabled );
}
