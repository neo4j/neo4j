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
package org.neo4j.server.security.ssl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KeyStoreFactoryTest
{
    @Rule
    public TemporaryFolder dir = new TemporaryFolder();

    @Test
    public void shouldCreateKeyStoreForGivenKeyPair() throws Exception
    {
        // given
        File certificatePath = new File(dir.getRoot(), "cert" );
        File privateKeyPath =  new File(dir.getRoot(), "key" );

        new Certificates().createSelfSignedCertificate( certificatePath, privateKeyPath, "some-hostname" );

        // when
        KeyStoreInformation ks = new KeyStoreFactory().createKeyStore( privateKeyPath, certificatePath );

        // then
        assertNotNull( ks.getKeyStore() );
    }

    @Test
    public void shouldImportSingleCertificateWhenNotInAChain() throws Exception
    {
        // given
        File certificatePath = new File(dir.getRoot(), "cert" );
        File privateKeyPath = new File(dir.getRoot(),"key" );

        new Certificates().createSelfSignedCertificate( certificatePath, privateKeyPath, "some-hostname" );

        KeyStoreInformation keyStoreInformation = new KeyStoreFactory().createKeyStore( privateKeyPath,
                certificatePath );

        KeyStore keyStore = keyStoreInformation.getKeyStore();

        // when
        Certificate[] chain = keyStore.getCertificateChain( "key" );

        // then
        assertEquals( "Single certificate expected not a chain of [" + chain.length + "]", 1, chain.length );
    }

    @Test
    public void shouldImportAllCertificatesInAChain() throws Exception
    {
        // given
        File privateKeyPath = fileFromResources( "/certificates/chained_key.der" );
        File certificatePath = fileFromResources( "/certificates/combined.pem" );
        KeyStoreInformation keyStoreInformation = new KeyStoreFactory().createKeyStore( privateKeyPath,
                certificatePath );

        KeyStore keyStore = keyStoreInformation.getKeyStore();

        // when
        Certificate[] chain = keyStore.getCertificateChain( "key" );

        // then
        assertEquals( "3 certificates expected in chain: root, intermediary, and user's", 3, chain.length );
    }

    private File fileFromResources( String path )
    {
        URL url = this.getClass().getResource( path );
        return new File( url.getFile() );
    }

}
