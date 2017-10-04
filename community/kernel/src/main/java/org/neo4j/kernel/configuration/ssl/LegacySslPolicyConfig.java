/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.configuration.ssl;

import java.io.File;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.pathSetting;

/**
 * Deprecated in favour of {@link SslPolicyConfig}.
 */
@Deprecated
@Description( "Legacy SSL policy settings" )
public class LegacySslPolicyConfig implements LoadableConfig
{
    public static final String LEGACY_POLICY_NAME = "legacy";

    @Deprecated
    @Description( "Directory for storing certificates to be used by Neo4j for TLS connections" )
    public static Setting<File> certificates_directory =
            pathSetting( "dbms.directories.certificates", "certificates" );

    @Deprecated
    @Internal
    @Description( "Path to the X.509 public certificate to be used by Neo4j for TLS connections" )
    public static Setting<File> tls_certificate_file =
            derivedSetting( "unsupported.dbms.security.tls_certificate_file", certificates_directory,
                    certificates -> new File( certificates, "neo4j.cert" ), PATH );

    @Deprecated
    @Internal
    @Description( "Path to the X.509 private key to be used by Neo4j for TLS connections" )
    public static final Setting<File> tls_key_file =
            derivedSetting( "unsupported.dbms.security.tls_key_file", certificates_directory,
                    certificates -> new File( certificates, "neo4j.key" ), PATH );
}
