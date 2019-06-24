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
package org.neo4j.string;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_16;
import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class SecureStringTest
{
    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void testString( boolean encrypt )
    {
        testString( "a super secret text", encrypt );
    }

    @TestFactory
    Collection<DynamicTest> dynamicTestsFromCollection()
    {
        Random random = new Random();
        Collection<Charset> standardCharsets = Arrays.asList( US_ASCII, UTF_8, UTF_16, ISO_8859_1, UTF_16BE, UTF_16LE );
        Collection<DynamicTest> tests = new ArrayList<>();

        for ( Charset charset : standardCharsets )
        {
            for ( boolean encrypted : Arrays.asList( true, false ) )
            {
                long randomSeed = random.nextLong();
                String testName = String.format( "TestRandomStrings: encrypted(%s), encoding(%s), randomSeed(%d)", encrypted, charset, randomSeed );
                tests.add( dynamicTest( testName, () -> testRandomStrings( encrypted, charset, randomSeed ) ) );
            }
        }
        return tests;
    }

    static void testRandomStrings( boolean encrypt, Charset charset, long randomSeed )
    {
        Random random = new Random( randomSeed );
        for ( int i = 0; i < 1000; i++ )
        {
            byte[] bytes = new byte[random.nextInt( 10000 ) + 1];
            random.nextBytes( bytes );
            String clearText = new String( bytes, charset );
            testString( clearText, encrypt );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void testNullAndEmpty( boolean encrypt )
    {
        testString( "", encrypt );
        testString( null, encrypt );
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void toStringNotLeaking( boolean encrypt )
    {
        String clearText = "leaked?";
        SecureString ss = new SecureString( clearText, encrypt );
        assertNotEquals( clearText, ss.toString() );
    }

    private static void testString( String clearText, boolean useEncryption )
    {
        SecureString ss = new SecureString( clearText, useEncryption );
        if ( useEncryption )
        {
            assertTrue( ss.encryptionAvailable() );
        }
        assertEquals( clearText, ss.getString() );
    }
}
