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

import java.io.File;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Group;
import org.neo4j.configuration.GroupSettingSupport;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.Settings.BOOLEAN;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.NO_DEFAULT;
import static org.neo4j.configuration.Settings.STRING;
import static org.neo4j.configuration.Settings.setting;

@ServiceProvider
@Group( "dbms.ssl.policy" )
public class PemSslPolicyConfig extends BaseSslPolicyConfig implements LoadableConfig
{
    @Description( "Private PKCS#8 key in PEM format." )
    public final Setting<File> private_key;

    @Description( "X.509 certificate (chain) of this server in PEM format." )
    public final Setting<File> public_certificate;

    @Description( "Path to directory of X.509 certificates in PEM format for trusted parties." )
    public final Setting<File> trusted_dir;

    @Internal // not yet implemented
    @Description( "The password for the private key." )
    public final Setting<String> private_key_password;

    @Description( "Allows the generation of a private key and associated self-signed certificate." +
                  " Only performed when both objects cannot be found." )
    public final Setting<Boolean> allow_key_generation;

    public PemSslPolicyConfig()
    {
        this( "<pem-policyname>" );
    }

    public PemSslPolicyConfig( String policyName )
    {
        super( new GroupSettingSupport( PemSslPolicyConfig.class, policyName ), Format.PEM );

        this.private_key = group.scope( derivedDefault( "private_key", base_directory, "private.key" ) );
        this.public_certificate = group.scope( derivedDefault( "public_certificate", base_directory, "public.crt" ) );
        this.trusted_dir = group.scope( derivedDefault( "trusted_dir", base_directory, "trusted" ) );
        this.private_key_password = group.scope( setting( "private_key_password", STRING, NO_DEFAULT ) );
        this.allow_key_generation = group.scope( setting( "allow_key_generation", BOOLEAN, FALSE ) );
    }
}
