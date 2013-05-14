/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.security;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestKeyStoreFactory {

    @Test
    public void shouldCreateKeyStoreForGivenKeyPair() throws Exception {

        File cPath = File.createTempFile("cert", "test");
        File pkPath = File.createTempFile("privatekey", "test");
        File keyStorePath = File.createTempFile("keyStore", "test");
        
        SslCertificateFactory ssl = new SslCertificateFactory();
        ssl.createSelfSignedCertificate(cPath, pkPath, "asd");
        
        KeyStoreFactory keyStoreFactory = new KeyStoreFactory();
        
        KeyStoreInformation ks = keyStoreFactory.createKeyStore(keyStorePath, pkPath, cPath);
        
        File keyStoreFile = new File(ks.getKeyStorePath());
        assertThat(keyStoreFile.exists(), is(true));
        
    }

    @Test
    public void shouldCertFileWithMultipleCertificatesImportAll() throws Exception {

        File keyStorePath = File.createTempFile("keyStore", "test");
        File privateKeyPath = fileFromResources("/certificates/chained_key.der");
        File certificatePath = fileFromResources("/certificates/combined.pem");
        KeyStoreFactory keyStoreFactory = new KeyStoreFactory();
        KeyStoreInformation keyStoreInformation = keyStoreFactory.createKeyStore(keyStorePath, privateKeyPath, certificatePath);

        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(keyStoreInformation.getKeyStorePath()), keyStoreInformation.getKeyStorePassword());

        Certificate[] chain = keyStore.getCertificateChain("key");

        assertThat("expecting more than 1 certificate in chain", chain.length, greaterThan(1));
    }

    private File fileFromResources(String path) {
        URL url = this.getClass().getResource(path);
        return new File(url.getFile());
    }
    
}
