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
package org.neo4j.kernel.impl.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

@Ignore( "Not used anymore" )
public abstract class TestShortString
{

    @Test
    public void canEncodeEmptyString() throws Exception
    {
        assertCanEncode( "" );
    }

    @Test
    public void canEncodeReallyLongString() throws Exception
    {
        assertCanEncode( "                    " ); // 20 spaces
        assertCanEncode( "                " ); // 16 spaces
    }

    @Test
    public void canEncodeFifteenSpaces() throws Exception
    {
        assertCanEncode( "               " );
    }

    @Test
    public void canEncodeNumericalString() throws Exception
    {
        assertCanEncode( "0123456789+,'.-" );
        assertCanEncode( " ,'.-0123456789" );
        assertCanEncode( "+ '.0123456789-" );
        assertCanEncode( "+, 0123456789.-" );
        assertCanEncode( "+,0123456789' -" );
        assertCanEncode( "+0123456789,'. " );
        // IP(v4) numbers
        assertCanEncode( "192.168.0.1" );
        assertCanEncode( "127.0.0.1" );
        assertCanEncode( "255.255.255.255" );
    }

    @Test
    public void canEncodeTooLongStringsWithCharsInDifferentTables()
            throws Exception
    {
        assertCanEncode( "____________+" );
        assertCanEncode( "_____+_____" );
        assertCanEncode( "____+____" );
        assertCanEncode( "HELLO world" );
        assertCanEncode( "Hello_World" );
    }

    @Test
    public void canEncodeUpToNineEuropeanChars() throws Exception
    {
        // Shorter than 10 chars
        assertCanEncode( "fågel" ); // "bird" in Swedish
        assertCanEncode( "påfågel" ); // "peacock" in Swedish
        assertCanEncode( "påfågelö" ); // "peacock island" in Swedish
        assertCanEncode( "påfågelön" ); // "the peacock island" in Swedish
        // 10 chars
        assertCanEncode( "påfågelöar" ); // "peacock islands" in Swedish
    }

    @Test
    public void canEncodeEuropeanCharsWithPunctuation() throws Exception
    {
        assertCanEncode( "qHm7 pp3" );
        assertCanEncode( "UKKY3t.gk" );
    }

    @Test
    public void canEncodeAlphanumerical() throws Exception
    {
        assertCanEncode( "1234567890" ); // Just a sanity check
        assertCanEncodeInBothCasings( "HelloWor1d" ); // There is a number there
        assertCanEncode( "          " ); // Alphanum is the first that can encode 10 spaces
        assertCanEncode( "_ _ _ _ _ " ); // The only available punctuation
        assertCanEncode( "H3Lo_ or1D" ); // Mixed case + punctuation
        assertCanEncode( "q1w2e3r4t+" ); // + is not in the charset
    }

    @Test
    public void canEncodeHighUnicode() throws Exception
    {
        assertCanEncode( "\u02FF" );
        assertCanEncode( "hello\u02FF" );
    }

    @Test
    public void canEncodeLatin1SpecialChars() throws Exception
    {
        assertCanEncode( "#$#$#$#" );
        assertCanEncode( "$hello#" );
    }

    @Test
    public void canEncodeTooLongLatin1String() throws Exception
    {
        assertCanEncode( "#$#$#$#$" );
    }

    @Test
    public void canEncodeLowercaseAndUppercaseStringsUpTo12Chars() throws Exception
    {
        assertCanEncodeInBothCasings( "hello world" );
        assertCanEncode( "hello_world" );
        assertCanEncode( "_hello_world" );
        assertCanEncode( "hello::world" );
        assertCanEncode( "hello//world" );
        assertCanEncode( "hello world" );
        assertCanEncode( "http://ok" );
        assertCanEncode( "::::::::" );
        assertCanEncode( " _.-:/ _.-:/" );
    }

    // === test utils ===

    private void assertCanEncodeInBothCasings( String string )
    {
        assertCanEncode( string.toLowerCase() );
        assertCanEncode( string.toUpperCase() );
    }

    abstract protected void assertCanEncode( String string );

    // === Micro benchmarking === [includes random tests]

    public static void main( String[] args )
    {
        microbench( 10, TimeUnit.SECONDS, Charset.UNIFORM_ASCII );
        microbench( 10, TimeUnit.SECONDS, Charset.SYMBOLS );
        microbench( 10, TimeUnit.SECONDS, Charset.LONG );
        microbench( 10, TimeUnit.SECONDS, Charset.INT );
        microbench( 10, TimeUnit.SECONDS, Charset.UNIFORM_LATIN );
        microbench( 10, TimeUnit.SECONDS, Charset.UNICODE );
    }

    @SuppressWarnings( "boxing" )
    private static void microbench( long time, TimeUnit unit, Charset charset )
    {
        long successes = 0, failures = 0, errors = 0;
        long remaining = time = unit.toMillis( time );
        while ( remaining > 0 )
        {
            List<String> strings = randomStrings( 1000, charset, 15 );
            long start = System.currentTimeMillis();
            for ( String string : strings )
            {
                String result = roundtrip( string );
                if ( result != null )
                {
                    if ( string.equals( result ) )
                    {
                        successes++;
                    }
                    else
                    {
                        errors++;
                        System.out.printf( "Expected: %s, got: %s%n", string, result );
                    }
                }
                else
                {
                    failures++;
                }
            }
            remaining -= System.currentTimeMillis() - start;
        }
        time -= remaining;
        System.out.printf( "=== %s ===%n", charset.name() );
        System.out.printf( "%s successful, %s non-convertable, %s misconverted%n", successes, failures, errors );
        long total = successes + failures + errors;
        System.out.printf( "%.3f conversions per ms%n", total / (double) time );
        System.out.printf( "%.2f%% success rate%n", 100 * ( successes / ( (double) ( total ) ) ) );
    }

    private static String roundtrip( @SuppressWarnings("UnusedParameters") String string )
    {
        return null;
    }

    public static List<String> randomStrings( int count, Charset charset, int maxLen )
    {
        List<String> result = new ArrayList<String>( count );
        for ( int i = 0; i < count; i++ )
        {
            result.add( charset.randomString( maxLen ) );
        }
        return result;
    }

    private static Random random = new Random();

    public static enum Charset
    {
        UNIFORM_ASCII
        {
            @Override
            String randomString( int maxLen )
            {
                char[] chars = new char[random.nextInt( maxLen + 1 )];
                for ( int i = 0; i < chars.length; i++ )
                {
                    chars[i] = (char) ( 0x20 + random.nextInt( 94 ) );
                }
                return new String( chars );
            }
        },
        SYMBOLS
        {
            @Override
            String randomString( int maxLen )
            {
                char[] chars = new char[random.nextInt( maxLen + 1 )];
                for ( int i = 0; i < chars.length; i++ )
                {
                    chars[i] = SYMBOL_CHARS[random.nextInt( SYMBOL_CHARS.length )];
                }
                return new String( chars );
            }
        },
        UNIFORM_LATIN
        {
            @Override
            String randomString( int maxLen )
            {
                char[] chars = new char[random.nextInt( maxLen + 1 )];
                for ( int i = 0; i < chars.length; i++ )
                {
                    chars[i] = (char) ( 0x20 + random.nextInt( 0xC0 ) );
                    if ( chars[i] > 0x7f ) chars[i] += 0x20;
                }
                return new String( chars );
            }
        },
        LONG
        {
            @Override
            String randomString( int maxLen )
            {
                return Long.toString( random.nextLong() % ( (long) Math.pow( 10, maxLen ) ) );
            }
        },
        INT
        {
            @Override
            String randomString( int maxLen )
            {
                return Long.toString( random.nextInt() );
            }
        },
        UNICODE
        {
            @Override
            String randomString( int maxLen )
            {
                char[] chars = new char[random.nextInt( maxLen + 1 )];
                for ( int i = 0; i < chars.length; i++ )
                {
                    chars[i] = (char) ( 1 + random.nextInt( 0xD7FE ) );
                }
                return new String( chars );
            }
        },
        ;
        static char[] SYMBOL_CHARS = new char[26 + 26 + 10 + 1];
        static
        {
            SYMBOL_CHARS[0] = '_';
            int i = 1;
            for ( char c = '0'; c <= '9'; c++ )
            {
                SYMBOL_CHARS[i++] = c;
            }
            for ( char c = 'A'; c <= 'Z'; c++ )
            {
                SYMBOL_CHARS[i++] = c;
            }
            for ( char c = 'a'; c <= 'z'; c++ )
            {
                SYMBOL_CHARS[i++] = c;
            }
        }

        abstract String randomString( int maxLen );
    }
}
