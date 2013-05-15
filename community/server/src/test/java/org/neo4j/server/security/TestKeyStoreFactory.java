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

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestKeyStoreFactory
{
    @Test
    public void shouldCreateKeyStoreForGivenKeyPair() throws Exception
    {
        SslCertificateFactory ssl = new SslCertificateFactory();
        ssl.createSelfSignedCertificate( cPath, pkPath, "asd" );

        KeyStoreFactory keyStoreFactory = new KeyStoreFactory();

        KeyStoreInformation ks = keyStoreFactory.createKeyStore( keyStorePath, pkPath, cPath );

        File keyStoreFile = new File( ks.getKeyStorePath() );
        assertThat( keyStoreFile.exists(), is( true ) );
        keyStorePath.delete();
    }
    
    private File cPath;
    private File pkPath;
    private File keyStorePath;
    
    @Before
    public void createFiles() throws IOException
    {
        cPath = File.createTempFile( "cert", "test" );
        pkPath = File.createTempFile( "privatekey", "test" );
        keyStorePath = File.createTempFile( "keyStore", "test" );
    }
    
    @After
    public void deleteFiles()
    {
        keyStorePath.delete();
        pkPath.delete();
        cPath.delete();
    }
}
