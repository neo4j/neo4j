/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.configuration;

import org.neo4j.kernel.configuration.BaseConfigurationMigrator;

public class ServerConfigurationMigrator extends BaseConfigurationMigrator
{
    {
        add( new PropertyRenamed(
                "org.neo4j.server.webserver.https.cert.location",
                "dbms.security.tls_certificate_file",
                "The TLS certificate configuration you are using, 'org.neo4j.server.webserver.https.cert.location' is " +
                "deprecated. Please use 'dbms.security.tls_certificate_file' instead."
        ));
        add( new PropertyRenamed(
                "org.neo4j.server.webserver.https.key.location",
                "dbms.security.tls_key_file",
                "The TLS key configuration you are using, " +
                "'org.neo4j.server.webserver.https.key.location' is deprecated, please use " +
                "'dbms.security.tls_key_file' instead."
        ));
    }
}
