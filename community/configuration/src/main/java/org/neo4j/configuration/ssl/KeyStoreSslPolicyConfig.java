/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.configuration.ssl;

import java.nio.file.Path;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.configuration.Description;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.string.SecureString;

import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SECURE_STRING;
import static org.neo4j.configuration.SettingValueParsers.STRING;

@PublicApi
public abstract class KeyStoreSslPolicyConfig extends SslPolicyConfig
{
    @Description( "File containing private key and certificate chain, managed with Java Keytool." )
    public final Setting<Path> keystore = getBuilder( "keystore", PATH, Path.of( ".keystore" ) ).setDependency( base_directory ).immutable().build();

    @Description( "The password for the keystore." )
    public final Setting<SecureString> keystore_pass = getBuilder( "keystore_pass", SECURE_STRING, null ).immutable().build();

    @Description( "File containing trusted certificates, managed by Java Keytool. Defaults to value of 'keystore'." )
    public final Setting<Path> truststore = getBuilder( "truststore", PATH, null ).setDependency( keystore ).build();

    @Description( "The password for the truststore." )
    public final Setting<SecureString> truststore_pass = getBuilder( "truststore_pass", SECURE_STRING, null ).setDependency( keystore_pass ).build();

    @Description( "The alias for the private key entry in the keystore, including the associated certificate chain." )
    public final Setting<String> entry_alias = getBuilder( "entry_alias", STRING, null ).build();

    @Description( "The password for the private key entry. Should not be set if format is PKCS12." )
    public final Setting<SecureString> entry_pass = getBuilder( "entry_pass", SECURE_STRING, null ).setDependency( keystore_pass ).build();

    protected KeyStoreSslPolicyConfig( String scope )
    {
        super( scope );
    }

    protected KeyStoreSslPolicyConfig()
    {
        super();
    }
}
