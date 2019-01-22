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
package org.neo4j.kernel.impl.store;

import java.util.Random;

public enum TestStringCharset
{
    UNIFORM_ASCII
            {
                @Override
                String randomString( int maxLen )
                {
                    char[] chars = new char[random.nextInt( maxLen + 1 )];
                    for ( int i = 0; i < chars.length; i++ )
                    {
                        chars[i] = (char) (0x20 + random.nextInt( 94 ));
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
                        chars[i] = (char) (0x20 + random.nextInt( 0xC0 ));
                        if ( chars[i] > 0x7f )
                        {
                            chars[i] += 0x20;
                        }
                    }
                    return new String( chars );
                }
            },
    LONG
            {
                @Override
                String randomString( int maxLen )
                {
                    return Long.toString( random.nextLong() % ((long) Math.pow( 10, maxLen )) );
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
                        chars[i] = (char) (1 + random.nextInt( 0xD7FE ));
                    }
                    return new String( chars );
                }
            },;
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

    private static Random random = new Random();

    abstract String randomString( int maxLen );
}
