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
package org.neo4j.kernel.configuration.ssl;

import java.io.File;
import java.util.List;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Group;
import org.neo4j.kernel.configuration.GroupSettingSupport;
import org.neo4j.ssl.ClientAuth;

import static java.lang.String.join;
import static java.util.Arrays.asList;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.pathSetting;
import static org.neo4j.kernel.configuration.Settings.setting;

@Group( "dbms.ssl.policy" )
public class SslPolicyConfig implements LoadableConfig
{
    public static final List<String> TLS_VERSION_DEFAULTS = asList( "TLSv1.2" );
    public static final List<String> CIPHER_SUITES_DEFAULTS = null;

    @Description( "The mandatory base directory for cryptographic objects of this policy." +
                  " It is also possible to override each individual configuration with absolute paths." )
    public final Setting<File> base_directory;

    @Description( "Allows the generation of a private key and associated self-signed certificate." +
                  " Only performed when both objects cannot be found." )
    public final Setting<Boolean> allow_key_generation;

    @Description( "Makes this policy trust all remote parties." +
                  " Enabling this is not recommended and the trusted directory will be ignored." )
    public final Setting<Boolean> trust_all;

    @Description( "Private PKCS#8 key in PEM format." )
    public final Setting<File> private_key;

    @Internal // not yet implemented
    @Description( "The password for the private key." )
    public final Setting<String> private_key_password;

    @Description( "X.509 certificate (chain) of this server in PEM format." )
    public final Setting<File> public_certificate;

    @Description( "Path to directory of X.509 certificates in PEM format for trusted parties." )
    public final Setting<File> trusted_dir;

    @Description( "Path to directory of CRLs (Certificate Revocation Lists) in PEM format." )
    public final Setting<File> revoked_dir;

    @Description( "Client authentication stance." )
    public final Setting<ClientAuth> client_auth;

    @Description( "Restrict allowed TLS protocol versions." )
    public final Setting<List<String>> tls_versions;

    @Description( "Restrict allowed ciphers." )
    public final Setting<List<String>> ciphers;

    public SslPolicyConfig()
    {
        this( "<policyname>" );
    }

    public SslPolicyConfig( String policyName )
    {
        GroupSettingSupport group = new GroupSettingSupport( SslPolicyConfig.class, policyName );

        this.base_directory = group.scope( pathSetting( "base_directory", NO_DEFAULT ) );
        this.allow_key_generation = group.scope( setting( "allow_key_generation", BOOLEAN, FALSE ) );
        this.trust_all = group.scope( setting( "trust_all", BOOLEAN, FALSE ) );

        this.private_key = group.scope( derivedDefault( "private_key", base_directory, "private.key" ) );
        this.public_certificate = group.scope( derivedDefault( "public_certificate", base_directory, "public.crt" ) );
        this.trusted_dir = group.scope( derivedDefault( "trusted_dir", base_directory, "trusted" ) );
        this.revoked_dir = group.scope( derivedDefault( "revoked_dir", base_directory, "revoked" ) );

        this.private_key_password = group.scope( setting( "private_key_password", STRING, NO_DEFAULT ) );
        this.client_auth = group.scope( setting( "client_auth", options( ClientAuth.class, true ), ClientAuth.REQUIRE.name() ) );
        this.tls_versions = group.scope( setting( "tls_versions", STRING_LIST, joinList( TLS_VERSION_DEFAULTS ) ) );
        this.ciphers = group.scope( setting( "ciphers", STRING_LIST, joinList( CIPHER_SUITES_DEFAULTS ) ) );
    }

    // TODO: can we make this handle relative paths?
    private Setting<File> derivedDefault( String settingName, Setting<File> baseDirectory, String defaultFilename )
    {
        return derivedSetting( settingName, baseDirectory, base -> new File( base, defaultFilename ), PATH );
    }

    private String joinList( List<String> list )
    {
        if ( list == null )
        {
            return null;
        }
        else
        {
            return join( ",", list );
        }
    }
}
