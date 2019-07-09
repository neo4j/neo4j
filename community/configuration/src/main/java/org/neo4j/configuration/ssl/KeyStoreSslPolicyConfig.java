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
import java.util.function.Function;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Group;
import org.neo4j.configuration.GroupSettingSupport;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.configuration.Settings;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.Settings.NO_DEFAULT;
import static org.neo4j.configuration.Settings.PATH;
import static org.neo4j.configuration.Settings.STRING;
import static org.neo4j.configuration.Settings.derivedSetting;
import static org.neo4j.configuration.Settings.setting;

@ServiceProvider
@Group( "dbms.ssl.policy" )
public class KeyStoreSslPolicyConfig extends BaseSslPolicyConfig implements LoadableConfig
{
    @Description( "File containing private key and certificate chain, managed with Java Keytool." )
    public final Setting<File> keystore;

    @Description( "The password for the keystore." )
    public final Setting<String> keystore_pass;

    @Description( "File containing trusted certificates, managed by Java Keytool. Defaults to value of 'keystore'." )
    public final Setting<File> truststore;

    @Description( "The password for the truststore." )
    public final Setting<String> truststore_pass;

    @Description( "The alias for the private key entry in the keystore, including the associated certificate chain." )
    public final Setting<String> entry_alias;

    @Description( "The password for the private key entry. Should not be set if format is PKCS12." )
    public final Setting<String> entry_pass;

    public KeyStoreSslPolicyConfig()
    {
        this( "<keystore_policyname>" );
    }

    public KeyStoreSslPolicyConfig( String policyName )
    {
        super( new GroupSettingSupport( KeyStoreSslPolicyConfig.class, policyName ), Format.PKCS12 );

        this.keystore = group.scope( derivedDefault( "keystore", base_directory, ".keystore" ) );
        this.keystore_pass = group.scope( setting( "keystore_pass", Settings.STRING, NO_DEFAULT ) );
        this.truststore = group.scope( derivedSetting( "truststore", keystore, Function.identity(), PATH ) );
        this.truststore_pass = group.scope( derivedSetting( "truststore_pass", keystore_pass, Function.identity(), STRING ) );
        this.entry_alias = group.scope( setting( "entry_alias", Settings.STRING, NO_DEFAULT ) );
        this.entry_pass = group.scope( derivedSetting( "entry_pass", keystore_pass, Function.identity(), STRING ) );
    }
}
