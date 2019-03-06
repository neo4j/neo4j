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
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Group;
import org.neo4j.configuration.GroupSettingSupport;
import org.neo4j.configuration.Settings;
import org.neo4j.graphdb.config.Setting;

import static java.lang.String.join;
import static java.util.Collections.singletonList;
import static org.neo4j.configuration.Settings.BOOLEAN;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.NO_DEFAULT;
import static org.neo4j.configuration.Settings.PATH;
import static org.neo4j.configuration.Settings.STRING_LIST;
import static org.neo4j.configuration.Settings.derivedSetting;
import static org.neo4j.configuration.Settings.optionsIgnoreCase;
import static org.neo4j.configuration.Settings.pathSetting;
import static org.neo4j.configuration.Settings.setting;

public abstract class BaseSslPolicyConfig
{
    public static final List<String> TLS_VERSION_DEFAULTS = singletonList( "TLSv1.2" );
    public static final List<String> CIPHER_SUITES_DEFAULTS = null;

    final GroupSettingSupport group;

    @Description( "Path to directory of CRLs (Certificate Revocation Lists) in PEM format." )
    public final Setting<File> revoked_dir;

    @Description( "The mandatory base directory for cryptographic objects of this policy." +
                  " It is also possible to override each individual configuration with absolute paths." )
    public final Setting<File> base_directory;

    @Description( "Format of private key and certificates. Determines which other settings are needed." )
    public final Setting<Format> format;

    @Description( "Makes this policy trust all remote parties." +
                  " Enabling this is not recommended and the trusted directory will be ignored." )
    public final Setting<Boolean> trust_all;

    @Description( "Client authentication stance." )
    public final Setting<ClientAuth> client_auth;

    @Description( "Restrict allowed TLS protocol versions." )
    public final Setting<List<String>> tls_versions;

    @Description( "Restrict allowed ciphers." )
    public final Setting<List<String>> ciphers;

    @Description( "When true, this node will verify the hostname of every other instance it connects to by comparing the address it used to connect with it " +
            "and the patterns described in the remote hosts public certificate Subject Alternative Names" )
    public final Setting<Boolean> verify_hostname;

    public BaseSslPolicyConfig( GroupSettingSupport group, Format defaultFormat )
    {
        this.group = group;

        this.base_directory = group.scope( pathSetting( "base_directory", NO_DEFAULT ) );
        this.format = group.scope( setting( "format", Settings.optionsIgnoreCase( Format.class), defaultFormat.name() ) );

        this.trust_all = group.scope( setting( "trust_all", BOOLEAN, FALSE ) );

        this.client_auth = group.scope( setting( "client_auth", optionsIgnoreCase( ClientAuth.class ), ClientAuth.REQUIRE.name() ) );
        this.tls_versions = group.scope( setting( "tls_versions", STRING_LIST, joinList( TLS_VERSION_DEFAULTS ) ) );
        this.ciphers = group.scope( setting( "ciphers", STRING_LIST, joinList( CIPHER_SUITES_DEFAULTS ) ) );
        this.verify_hostname = group.scope( setting( "verify_hostname", BOOLEAN, FALSE ) );
        this.revoked_dir = group.scope( derivedDefault( "revoked_dir", base_directory, "revoked" ) );
    }

    // TODO: can we make this handle relative paths?
    Setting<File> derivedDefault( String settingName, Setting<File> baseDirectory, String defaultFilename )
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

    public enum Format
    {
        PEM, JKS, PKCS12
    }

    @Group( "dbms.ssl.policy" )
    public static class StubSslPolicyConfig extends BaseSslPolicyConfig
    {
        public StubSslPolicyConfig( String policyName )
        {
            super( new GroupSettingSupport( StubSslPolicyConfig.class, policyName ), Format.PEM );
        }
    }
}
