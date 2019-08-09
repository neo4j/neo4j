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
package org.neo4j.configuration;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.neo4j.configuration.ssl.JksSslPolicyConfig;
import org.neo4j.configuration.ssl.PemSslPolicyConfig;
import org.neo4j.configuration.ssl.Pkcs12SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.logging.AssertableLogProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class SettingMigratorsTest
{
    @Test
    void shouldRemoveAllowKeyGenerationFrom35ConfigFormat() throws Throwable
    {
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", TRUE );
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", FALSE );
    }

    @Test
    void shouldRemoveAllowKeyGeneration() throws Throwable
    {
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.pem.default.allow_key_generation", TRUE );
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.pem.default.allow_key_generation", FALSE );
    }

    @TestFactory
    Collection<DynamicTest> shouldMigrateSslPolicySettingToActualPolicyGroupName()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        Map<String,SslPolicyScope> sources = Map.of(
                "bolt.ssl_policy", SslPolicyScope.BOLT,
                "https.ssl_policy", SslPolicyScope.HTTPS,
                "dbms.backup.ssl_policy", SslPolicyScope.BACKUP,
                "causal_clustering.ssl_policy", SslPolicyScope.CLUSTER
        );
        sources.forEach( ( setting, source ) ->
        {
            String name = "Test migration of SslPolicy %s from source %s";
            tests.add( dynamicTest( String.format( name, "pem", source.name() ), () ->
                    testMigrateSslPolicy( setting, "pem", PemSslPolicyConfig.forScope( source ) ) ) );
            tests.add( dynamicTest( String.format( name, "jks", source.name() ), () ->
                    testMigrateSslPolicy( setting, "jks", JksSslPolicyConfig.forScope( source ) ) ) );
            tests.add( dynamicTest( String.format( name, "pkcs12", source.name() ), () ->
                    testMigrateSslPolicy( setting, "pkcs12", Pkcs12SslPolicyConfig.forScope( source ) ) ) );

        } );

        return tests;
    }

    @Test
    void shouldWarnWhenUsingLegacySslPolicySettings()
    {
        Map<String,String> legacySettings = Map.of(
                "dbms.directories.certificates", "/cert/dir/",
                "unsupported.dbms.security.tls_certificate_file", "public.crt",
                "unsupported.dbms.security.tls_key_file", "private.key" );

        var config = Config.newBuilder().setRaw( legacySettings ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        for ( String setting : legacySettings.keySet() )
        {
            logProvider.assertAtLeastOnce( inLog( Config.class ).warn("Use of deprecated setting %s. Legacy ssl policy is no longer supported.", setting ) );
        }

    }

    private static void testMigrateSslPolicy( String oldGroupnameSetting, String sslType, SslPolicyConfig policyConfig )
    {
        String oldFormatSetting = String.format( "dbms.ssl.policy.%s.foo.trust_all", sslType );
        var config = Config.newBuilder().setRaw( Map.of( oldGroupnameSetting, "foo", oldFormatSetting, "true" ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertTrue( config.get( policyConfig.trust_all ) );

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s.", oldGroupnameSetting ) );
        logProvider.assertAtLeastOnce( inLog( Config.class )
                .warn( "Use of deprecated setting %s. It is replaced by %s", oldFormatSetting, policyConfig.trust_all.name() ) );
    }

    private void shouldRemoveAllowKeyGeneration( String toRemove, String value )
    {
        var config = Config.newBuilder().setRaw( Map.of( toRemove, value ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( toRemove ) );

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Setting %s is removed. A valid key and certificate are required " +
                "to be present in the key and certificate path configured in this ssl policy.", toRemove ) );
    }
}
