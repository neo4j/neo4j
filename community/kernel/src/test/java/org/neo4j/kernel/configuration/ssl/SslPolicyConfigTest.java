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

import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.ssl.ClientAuth;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class SslPolicyConfigTest
{
    @Test
    public void shouldFindPolicyDefaults() throws Exception
    {
        // given
        Config config = Config.empty();
        Map<String,String> params = stringMap();

        String policyName = "XYZ";
        SslPolicyConfig policyConfig = new SslPolicyConfig( policyName );

        params.put( GraphDatabaseSettings.neo4j_home.name(), "/home" );
        params.put( policyConfig.base_directory.name(), "certificates/XYZ" );
        config.augment( params );

        // when
        File privateKey = policyConfig.private_key.from( config );
        File publicCertificate = policyConfig.public_certificate.from( config );
        File trustedDir = policyConfig.trusted_dir.from( config );
        File revokedDir = policyConfig.revoked_dir.from( config );
        String privateKeyPassword = policyConfig.private_key_password.from( config );
        boolean allowKeyGeneration = policyConfig.allow_key_generation.from( config );
        boolean trustAll = policyConfig.trust_all.from( config );
        List<String> tlsVersions = policyConfig.tls_versions.from( config );
        List<String> ciphers = policyConfig.ciphers.from( config );
        ClientAuth clientAuth = policyConfig.client_auth.from( config );

        // then
        assertEquals( new File( "/home/certificates/XYZ/private.key" ), privateKey );
        assertEquals( new File( "/home/certificates/XYZ/public.crt" ), publicCertificate );
        assertEquals( new File( "/home/certificates/XYZ/trusted" ), trustedDir );
        assertEquals( new File( "/home/certificates/XYZ/revoked" ), revokedDir );
        assertEquals( null, privateKeyPassword );
        assertFalse( allowKeyGeneration );
        assertFalse( trustAll );
        assertEquals( asList( "TLSv1.2", "TLSv1.1", "TLSv1" ), tlsVersions );
        assertNull( ciphers );
        assertEquals( ClientAuth.REQUIRE, clientAuth );
    }

    @Test
    public void shouldFindPolicyOverrides() throws Exception
    {
        // given
        Config config = Config.empty();
        Map<String,String> params = stringMap();

        String policyName = "XYZ";
        SslPolicyConfig policyConfig = new SslPolicyConfig( policyName );

        params.put( GraphDatabaseSettings.neo4j_home.name(), "/home" );
        params.put( policyConfig.base_directory.name(), "certificates/XYZ" );

        params.put( policyConfig.private_key.name(), "/path/to/my.key" );
        params.put( policyConfig.public_certificate.name(), "/path/to/my.crt" );

        params.put( policyConfig.trusted_dir.name(), "/some/other/path/to/trusted" );
        params.put( policyConfig.revoked_dir.name(), "/some/other/path/to/revoked" );

        params.put( policyConfig.allow_key_generation.name(), "true" );
        params.put( policyConfig.trust_all.name(), "true" );

        params.put( policyConfig.private_key_password.name(), "setecastronomy" );
        params.put( policyConfig.tls_versions.name(), "TLSv1.1,TLSv1.2" );
        params.put( policyConfig.ciphers.name(), "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" );
        params.put( policyConfig.client_auth.name(), "optional" );

        config.augment( params );

        // when
        File privateKey = policyConfig.private_key.from( config );
        File publicCertificate = policyConfig.public_certificate.from( config );
        File trustedDir = policyConfig.trusted_dir.from( config );
        File revokedDir = policyConfig.revoked_dir.from( config );
        String privateKeyPassword = policyConfig.private_key_password.from( config );
        boolean allowKeyGeneration = policyConfig.allow_key_generation.from( config );
        boolean trustAll = policyConfig.trust_all.from( config );
        List<String> tlsVersions = policyConfig.tls_versions.from( config );
        List<String> ciphers = policyConfig.ciphers.from( config );
        ClientAuth clientAuth = policyConfig.client_auth.from( config );

        // then
        assertEquals( new File( "/path/to/my.key" ), privateKey );
        assertEquals( new File( "/path/to/my.crt" ), publicCertificate );
        assertEquals( new File( "/some/other/path/to/trusted" ), trustedDir );
        assertEquals( new File( "/some/other/path/to/revoked" ), revokedDir );
        assertTrue( allowKeyGeneration );
        assertTrue( trustAll );
        assertEquals( "setecastronomy", privateKeyPassword );
        assertEquals( asList( "TLSv1.1", "TLSv1.2" ), tlsVersions );
        assertEquals( asList( "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" ), ciphers );
        assertEquals( ClientAuth.OPTIONAL, clientAuth );
    }

    @Test
    public void shouldFailWithIncompletePathOverrides() throws Exception
    {
        // given
        Config config = Config.empty();
        Map<String,String> params = stringMap();

        String policyName = "XYZ";
        SslPolicyConfig policyConfig = new SslPolicyConfig( policyName );

        params.put( GraphDatabaseSettings.neo4j_home.name(), "/home" );
        params.put( policyConfig.base_directory.name(), "certificates" );

        params.put( policyConfig.private_key.name(), "my.key" );
        params.put( policyConfig.public_certificate.name(), "path/to/my.crt" );

        config.augment( params );

        // when/then
        try
        {
            policyConfig.private_key.from( config );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }

        try
        {
            policyConfig.public_certificate.from( config );
            fail();
        }
        catch ( IllegalArgumentException e )
        {
            // expected
        }
    }
}
