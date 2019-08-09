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
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.string.SecureString;

import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SECURE_STRING;

@ServiceProvider
@PublicApi
public class PemSslPolicyConfig extends SslPolicyConfig
{
    @Description( "Private PKCS#8 key in PEM format." )
    public final Setting<Path> private_key = getBuilder( "private_key", PATH, Path.of( "private.key" ) )
            .setDependency( base_directory )
            .build();

    @Description( "X.509 certificate (chain) of this server in PEM format." )
    public final Setting<Path> public_certificate = getBuilder( "public_certificate", PATH, Path.of( "public.crt" ) )
            .setDependency( base_directory )
            .build();

    @Description( "Path to directory of X.509 certificates in PEM format for trusted parties." )
    public final Setting<Path> trusted_dir = getBuilder( "trusted_dir", PATH, Path.of( "trusted" ) )
            .setDependency( base_directory )
            .build();

    @Internal
    @Description( "The password for the private key." )
    public final Setting<SecureString> private_key_password = getBuilder( "private_key_password", SECURE_STRING, null ).build();

    public static PemSslPolicyConfig forScope( SslPolicyScope scope )
    {
        return new PemSslPolicyConfig( scope.name() );
    }

    private PemSslPolicyConfig( String scope )
    {
        super( scope );
    }
    public PemSslPolicyConfig()
    {
        super();  // For ServiceLoader
    }
    @Override
    public String getPrefix()
    {
        return super.getPrefix() + ".pem";
    }
}
