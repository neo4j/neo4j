/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.string;

import java.nio.charset.Charset;
import java.security.Key;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.neo4j.annotations.api.PublicApi;

import static java.nio.charset.StandardCharsets.UTF_8;

@PublicApi
public class SecureString
{
    private static final SecureRandom random = new SecureRandom();
    private Cipher cipher;

    private byte[] encryptedData;
    private boolean encryptionAvailable;

    private byte[] obfuscationKey;
    private static final Charset encoding = UTF_8;
    private static final int KEY_SIZE = 16;

    public SecureString( String stringToSecure )
    {
        this( stringToSecure, true );
    }

    SecureString( String stringToSecure, boolean tryUseEncryption )
    {
        this( stringToSecure != null ? stringToSecure.getBytes( encoding ) : null, tryUseEncryption );
    }

    private SecureString( byte[] dataToSecure, boolean tryUseEncryption )
    {
        if ( dataToSecure != null )
        {
            if ( tryUseEncryption )
            {
                try
                {
                    // Setup key
                    final byte[] key = new byte[KEY_SIZE];
                    random.nextBytes( key );
                    final Key aesKey = new SecretKeySpec( key, "AES" );
                    Arrays.fill( key, (byte) 0 );

                    // Setup iv
                    final byte[] iv = new byte[12];
                    random.nextBytes( iv );
                    GCMParameterSpec parameterSpec = new GCMParameterSpec( 128, iv );
                    Arrays.fill( iv, (byte) 0 );

                    // Init cypher and encode
                    cipher = Cipher.getInstance( "AES/GCM/NoPadding" );
                    cipher.init( Cipher.ENCRYPT_MODE, aesKey, parameterSpec );
                    encryptedData = cipher.doFinal( dataToSecure );

                    // Swap to decode mode
                    cipher.init( Cipher.DECRYPT_MODE, aesKey, parameterSpec );
                    encryptionAvailable = true;
                }
                catch ( Exception e )
                {
                    encryptionAvailable = false;
                }
            }

            if ( !encryptionAvailable )
            {
                //Using simple obfuscation
                obfuscationKey = new byte[KEY_SIZE];
                random.nextBytes( obfuscationKey );
                encryptedData = XOR( dataToSecure, obfuscationKey );
            }
        }
        else
        {
            encryptedData = null;
            encryptionAvailable = tryUseEncryption;
        }
    }

    boolean encryptionAvailable()
    {
        return encryptionAvailable;
    }

    private byte[] getData()
    {
        if ( encryptedData == null )
        {
            return null;
        }

        if ( encryptionAvailable() )
        {
            try
            {
                return cipher.doFinal( encryptedData );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Data corruption, could not decrypt data", e );
            }
        }
        return XOR( encryptedData, obfuscationKey );
    }

    private static byte[] XOR( final byte[] input, final byte[] key )
    {
        byte[] output = Arrays.copyOf( input, input.length );
        for ( int i = 0; i < input.length; i++ )
        {
            output[i] = (byte) (input[i] ^ key[i % key.length]);
        }

        return output;
    }

    public String getString()
    {
        byte[] data = getData();
        return data != null ? new String( data, encoding ) : null;
    }

    @Override
    public String toString()
    {
        return "################";
    }
}
