/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.UnsupportedEncodingException;
import java.util.EnumSet;

import org.neo4j.helpers.Pair;

/**
 * Supports encoding alphanumerical and <code>SP . - + , ' : / _</code>
 * 
 * (This version assumes 14bytes property block, instead of 8bytes)
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 */
public enum LongerShortString
{
    /**
     * Binary coded decimal with punctuation.
     *
     * <pre>
     * HEADER (binary): 0000 LENG PAD DATA... (0-25 chars) [5bit LENG] [3bit PAD] [4bit DATA]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9  +  ,  ' SP  .  -
     * </pre>
     */
    NUMERICAL( 29, 0x0F, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            switch ( b )
            {
            case 0:
                return 0xA;
            case 2:
                return 0xB;
            case 3:
                return 0xC;
            case 6:
                return 0xD;
            case 7:
                return 0xE;
            case 8:
                return 0xF;
            default:
                throw cannotEncode( b );
            }
        }

        @Override
        int encPunctuation( byte b )
        {
            throw cannotEncode( b );
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint < 10 ) return (char) ( codePoint + '0' );
            return decPunctuation( ( codePoint - 10 + 6 ) );
        }

        @Override
        long header( int length )
        {
            return length << 3;
        }
    },
    /**
     * Binary coded decimal with punctuation.
     *
     * <pre>
     * HEADER (binary): 0001 LENG PAD DATA... (0-25 chars) [5bit LENG] [3bit PAD] [4bit DATA]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9  +  ,  : SP  .  -
     * </pre>
     */
    DATE( 29, 0x0F, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            switch ( b )
            {
            case 0:
                return 0xA;
            case 2:
                return 0xB;
            case 3:
                return 0xC;
            case 4:
                // TODO
                return 0;
            case 6:
                return 0xD;
            case 7:
                return 0xE;
            case 8:
                return 0xF;
            default:
                throw cannotEncode( b );
            }
        }

        @Override
        int encPunctuation( byte b )
        {
            throw cannotEncode( b );
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint < 10 ) return (char) ( codePoint + '0' );
            return decPunctuation( ( codePoint - 10 + 6 ) );
        }

        @Override
        long header( int length )
        {
            return length << 3;
        }
    },
    /**
     * Upper-case characters with punctuation.
     *
     * <pre>
     * HEADER (binary): 0010 LENG PAD DATA... (0-20 chars) [5bit LENG] [3bit PAD] [5bit DATA] 
     * HEADER (binary): 0011 PAD DATA... (21 chars) [3bit PAD] [5bit DATA]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  _  .  -  :  /
     * </pre>
     */
    UPPER( 24, 0x1F, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate( b ) - 0x40;
        }

        @Override
        int encPunctuation( byte b )
        {
            return b == 0 ? 0x40 : b + 0x5a;
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'A' - 1 );
            return decPunctuation( codePoint - 0x1A );
        }

        @Override
        long header( int length )
        {
            // shift to get padding
            if ( length == max ) return 0x30 << 3;
            return ( 0x20 | length ) << 3;
        }
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     * HEADER (binary): 0100 LENG PAD DATA... (0-20 chars) [5bit LENG] [3bit PAD] [5bit DATA]
     * HEADER (binary): 0101 PAD DATA... (21 chars) [3bit PAD] [5bit DATA]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  :  /
     * </pre>
     */
    LOWER( 24, 0x1F, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate( b ) - 0x60;
        }

        @Override
        int encPunctuation( byte b )
        {
            return b == 0 ? 0x60 : b + 0x7a;
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            return decPunctuation( codePoint - 0x1A );
        }

        @Override
        long header( int length )
        {
            // shift to get padding
            if ( length == max ) return 0x50 << 3;
            return ( 0x40 | length ) << 3;
        }
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     * HEADER (binary): 0100 LENG PAD DATA... (0-20 chars) [5bit LENG] [3bit PAD] [5bit DATA]
     * HEADER (binary): 0101 PAD DATA... (21 chars) [3bit PAD] [5bit DATA]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  ,  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  +  @
     * </pre>
     */
    EMAIL( 24, 0x1F, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate( b ) - 0x60;
        }

        @Override
        int encPunctuation( byte b )
        {
            return b == 0 ? 0x60 : b + 0x7a;
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            return decPunctuation( codePoint - 0x1A );
        }

        @Override
        long header( int length )
        {
            // shift to get padding
            if ( length == max ) return 0x50 << 3;
            return ( 0x40 | length ) << 3;
        }
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     * HEADER (binary): 0100 LENG PAD DATA... (0-20 chars) [5bit LENG] [3bit PAD] [5bit DATA]
     * HEADER (binary): 0101 PAD DATA... (21 chars) [3bit PAD] [5bit DATA]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  +  @
     * 2-  0  1  2  3  4  5  6  7    8  9  0  :  /  ,
     * 3-  
     * </pre>
     */
    EMAIL_PLUS( 20, 0x1F, 6 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate( b ) - 0x60;
        }

        @Override
        int encPunctuation( byte b )
        {
            return b == 0 ? 0x60 : b + 0x7a;
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            return decPunctuation( codePoint - 0x1A );
        }

        @Override
        long header( int length )
        {
            // shift to get padding
            if ( length == max ) return 0x50 << 3;
            return ( 0x40 | length ) << 3;
        }
    },
    /**
     * Alpha-numerical characters space and underscore.
     *
     * HEADER (binary): 0110 LENG PAD DATA... (17 chars) [5bit LENG] [1bit PAD] [6bit DATA]
     * HEADER (binary): 0111 DATA... (18 chars) [6bit DATA]
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  0  1  2  3  4
     * 2-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 3-  p  q  r  s  t  u  v  w    x  y  z  5  6  7  8  9
     * </pre>
     */
    ALPHANUM( 20, 0x3F, 6 )
    {
        @Override
        char decTranslate( byte codePoint )
        {
            return EUROPEAN.decTranslate( (byte) ( codePoint + 0x40 ) );
        }

        @Override
        int encTranslate( byte b )
        {
            // Punctuation is in the same places as European
            if ( b < 0x20 ) return encPunctuation( b ); // Punctuation
            // But the rest is transposed by 0x40
            return EUROPEAN.encTranslate( b ) - 0x40;
        }

        @Override
        int encPunctuation( byte b )
        {
            switch ( b )
            {
            case 0:
                return 0x00; // SPACE
            case 1:
                return 0x20; // UNDERSCORE
            default:
                throw cannotEncode( b );
            }
        }

        @Override
        long header( int length )
        {
            if ( length == max ) return 0x70;
            return ( 0x60 | length ) << 1;
        }
    },
    /**
     * Alpha-numerical characters space and underscore.
     *
     * HEADER (binary): 0110 LENG PAD DATA... (17 chars) [5bit LENG] [1bit PAD] [6bit DATA]
     * HEADER (binary): 0111 DATA... (18 chars) [6bit DATA]
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  +  ,  .  -  /
     * 2-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 3-  p  q  r  s  t  u  v  w    x  y  z  '  :  @
     * </pre>
     */
    ALPHA_SYMBOL( 20, 0x3F, 6 )
    {
        @Override
        char decTranslate( byte codePoint )
        {
            return EUROPEAN.decTranslate( (byte) ( codePoint + 0x40 ) );
        }

        @Override
        int encTranslate( byte b )
        {
            // Punctuation is in the same places as European
            if ( b < 0x20 ) return encPunctuation( b ); // Punctuation
            // But the rest is transposed by 0x40
            return EUROPEAN.encTranslate( b ) - 0x40;
        }

        @Override
        int encPunctuation( byte b )
        {
            switch ( b )
            {
            case 0:
                return 0x00; // SPACE
            case 1:
                return 0x20; // UNDERSCORE
            case 2: return 0;
            case 3: return 0;
            case 4: return 0;
            case 5: return 0;
            case 6: return 0;
            case 7: return 0;
            case 8: return 0;
            default:
                throw cannotEncode( b );
            }
        }

        @Override
        long header( int length )
        {
            if ( length == max ) return 0x70;
            return ( 0x60 | length ) << 1;
        }
    },
    /**
     * The most common European characters (latin-1 but with less punctuation).
     *
     * <pre>
     * HEADER (binary): 1000 LENG PAD DATA... (1-14 chars) [4bit LENG] [6bit PAD] [7bit DATA]
     * HEADER (binary): 1001 PAD DATA... (15 chars) [3bit PAD] [7bit DATA]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  À  Á  Â  Ã  Ä  Å  Æ  Ç    È  É  Ê  Ë  Ì  Í  Î  Ï
     * 1-  Ð  Ñ  Ò  Ó  Ô  Õ  Ö  .    Ø  Ù  Ú  Û  Ü  Ý  ,  ß
     * 2-  à  á  â  ã  ä  å  æ  ç    è  é  ê  ë  ì  í  î  ï
     * 3-  ð  ,  ò  ó  ô  õ  ö  -    ø  ù  ú  û  ü  ý  þ  ÿ
     * 4- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 5-  P  Q  R  S  T  U  V  W    X  Y  Z  0  1  2  3  4
     * 6-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 7-  p  q  r  s  t  u  v  w    x  y  z  5  6  7  8  9
     * </pre>
     */
    EUROPEAN( 17, 0x7F, 7 )
    {
        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint < 0x40 )
            {
                if ( codePoint == 0x17 ) return '.';
                if ( codePoint == 0x37 ) return '-';
                return (char) ( codePoint + 0xC0 );
            }
            else
            {
                if ( codePoint == 0x40 ) return ' ';
                if ( codePoint == 0x60 ) return '_';
                if ( codePoint >= 0x5B && codePoint < 0x60 ) return (char) ( '0' + codePoint - 0x5B );
                if ( codePoint >= 0x7B && codePoint < 0x80 ) return (char) ( '5' + codePoint - 0x7B );
                return (char) codePoint;
            }
        }

        @Override
        int encPunctuation( byte b )
        {
            switch ( b )
            {
            case 0x00:
                return 0x40; // SPACE
            case 0x01:
                return 0x60; // UNDERSCORE
            case 0x02:
                return 0x17; // DOT
            case 0x03:
                return 0x37; // DASH
            case 0x07:
                // TODO
                return 0;
            default:
                throw cannotEncode( b );
            }
        }

        @Override
        long header( int length )
        {
            if ( length == max ) return 0xF0 << 3;
            return ( 0xE0 | length ) << 6;
        }
    };
    
    private static final int BYTES = 16;

    final int max;
    final short mask;
    final short step;

    private LongerShortString( int max, int mask, int step )
    {
        this.max = max;
        this.mask = (short) mask;
        this.step = (short) step;
    }

    final IllegalArgumentException cannotEncode( byte b )
    {
        return new IllegalArgumentException( "Cannot encode as " + this.name() + ": " + b );
    }

    /** Lookup table for decoding punctuation */
    private static final char[] PUNCTUATION = { ' ', '_', '.', '-', ':', '/', ' ', '.', '-', '+', ',', '\'', };

    final char decPunctuation( int code )
    {
        return PUNCTUATION[code];
    }

    public static void main( String[] args )
    {
        tryEncode( "2009-01-03 33:22:11 +0200" );
        tryEncode( "mattias@neotech.com" );
        tryEncode( "top, left, right" );
        tryEncode( "Top, left, right" );
        tryEncode( "sam@37signals.com" );
    }

    private static void tryEncode( String string )
    {
        System.out.println( "trying '" + string + "' (" + string.length() + ")" );
        PropertyRecord record = new PropertyRecord( 0 );
        Pair<Boolean, String> encoded = LongerShortString.encode( string, record );
        if ( encoded.first() )
        {
            System.out.println( "Encoded '" + string + "' as " + encoded.other() );
        }
        else
        {
            System.out.println( "Could not encode '" + string + "'" );
        }
    }

    int encTranslate( byte b )
    {
        if ( b < 0 ) return ( 0xFF & b ) - 0xC0; // European chars
        if ( b < 0x20 ) return encPunctuation( b ); // Punctuation
        if ( b >= '0' && b <= '4' ) return 0x5B + b - '0'; // Numbers
        if ( b >= '5' && b <= '9' ) return 0x7B + b - '5'; // Numbers
        return b; // Alphabetical
    }

    abstract int encPunctuation( byte b );

    abstract char decTranslate( byte codePoint );

    abstract long header( int length );

    /**
     * Encodes a short string.
     *
     * @param string the string to encode.
     * @param target the property record to store the encoded string in
     * @return <code>true</code> if the string could be encoded as a short
     *         string, <code>false</code> if it couldn't.
     */
    /*
     * Intermediate code table
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  _  .  -  :  /  +  ,    '
     * 1-
     * 2-
     * 3-  0  1  2  3  4  5  6  7    8  9
     * 4-     A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 5-  P  Q  R  S  T  U  V  W    X  Y  Z
     * 6-     a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 7-  p  q  r  s  t  u  v  w    x  y  z
     * 8-
     * 9-
     * A-
     * B-
     * C-  À  Á  Â  Ã  Ä  Å  Æ  Ç    È  É  Ê  Ë  Ì  Í  Î  Ï
     * D-  Ð  Ñ  Ò  Ó  Ô  Õ  Ö       Ø  Ù  Ú  Û  Ü  Ý  Þ  ß
     * E-  à  á  â  ã  ä  å  æ  ç    è  é  ê  ë  ì  í  î  ï
     * F-  ð  ñ  ò  ó  ô  õ  ö       ø  ù  ú  û  ü  ý  þ  ÿ
     */
    public static Pair<Boolean,String> encode( String string, PropertyRecord target )
    {
        if ( string.length() > 29 ) return Pair.of( false, null ); // Not handled by any encoding
        if ( string.equals( "" ) )
        {
            target.setPropBlock( 0 );
            return Pair.of( true, NUMERICAL.name() );
        }
        // Keep track of the possible encodings that can be used for the string
        EnumSet<LongerShortString> possible = null;
        // First try encoding using Latin-1
        if ( string.length() < BYTES )
        {
            if ( encodeLatin1( string, target ) ) return Pair.of( true, "UTF8" );
            // If the string was short enough, but still didn't fit in latin-1
            // we know that no other encoding will work either, remember that
            // so that we can try UTF-8 at the end of this method
            possible = EnumSet.noneOf( LongerShortString.class );
        }
        // Allocate space for the intermediate representation
        // (using the intermediate representation table above)
        byte[] data = new byte[string.length()];
        if ( possible == null )
        {
            possible = EnumSet.allOf( LongerShortString.class );
            // ALPHANUM can only store len == 10
            if ( data.length > ALPHANUM.max ) possible.remove( ALPHANUM );
            if ( data.length > EUROPEAN.max ) possible.remove( EUROPEAN );
            if ( data.length > UPPER.max ) possible.removeAll( EnumSet.of( UPPER, LOWER, EMAIL ) );
        }
        LOOP: for ( int i = 0; i < data.length && !possible.isEmpty(); i++ )
        {
            char c = string.charAt( i );
            switch ( c )
            {
            case ' ':
                data[i] = 0;
                break;
            case '_':
                data[i] = 1;
                possible.removeAll( EnumSet.of( NUMERICAL, DATE ) );
                break;
            case '.':
                data[i] = 2;
                possible.remove( ALPHANUM );
                break;
            case '-':
                data[i] = 3;
                possible.remove( ALPHANUM );
                break;
            case ':':
                data[i] = 4;
                possible.removeAll( EnumSet.of( ALPHANUM, NUMERICAL, EUROPEAN, EMAIL ) );
                break;
            case '/':
                data[i] = 5;
                possible.removeAll( EnumSet.of( ALPHANUM, NUMERICAL, DATE, EUROPEAN, EMAIL ) );
                break;
            case '+':
                data[i] = 6;
                possible.retainAll( EnumSet.of( NUMERICAL, DATE, EMAIL ) );
                break;
            case ',':
                data[i] = 7;
                possible.retainAll( EnumSet.of( NUMERICAL, DATE, EMAIL, EUROPEAN ) );
                break;
            case '\'':
                data[i] = 8;
                possible.retainAll( EnumSet.of( NUMERICAL ) );
                break;
            case '@':
                possible.retainAll( EnumSet.of( EMAIL, EMAIL_PLUS ) );
                break;
            default:
                if ( ( c >= 'A' && c <= 'Z' ) )
                {
                    possible.removeAll( EnumSet.of( NUMERICAL, DATE, LOWER, EMAIL, EMAIL_PLUS ) );
                }
                else if ( ( c >= 'a' && c <= 'z' ) )
                {
                    possible.removeAll( EnumSet.of( NUMERICAL, DATE, UPPER ) );
                }
                else if ( ( c >= '0' && c <= '9' ) )
                {
                    possible.removeAll( EnumSet.of( UPPER, LOWER, EMAIL, ALPHA_SYMBOL ) );
                }
                else if ( c >= 'À' && c <= 'ÿ' && c != 0xD7 && c != 0xF7 )
                {
                    possible.retainAll( EnumSet.of( EUROPEAN ) );
                }
                else
                {
                    possible.clear();
                    break LOOP; // fall back to UTF-8
                }
                data[i] = (byte) c;
            }
        }
        for ( LongerShortString encoding : possible )
        {
            // Will return false if the data is too long for the encoding
            if ( encoding.doEncode( data, target ) ) return Pair.of( true, encoding.name() );
        }
        if ( string.length() < BYTES-1 )
        { // We might have a chance with UTF-8 - try it!
            try
            {
                return Pair.of( encodeUTF8( string.getBytes( "UTF-8" ), target ), "UTF8" );
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new IllegalStateException( "All JVMs must support UTF-8", e );
            }
        }
        return Pair.of( false, null );
    }

    /**
     * Decode a short string represented as a long
     *
     * @param data the value to decode to a short string.
     * @return the decoded short string
     */
    public static String decode( long data )
    {
        if ( data == 0 ) return "";
        int header = (int) ( data >>> (BYTES-1)*8 );
        LongerShortString table;
        switch ( header >>> 4 )
        {
        case 0: // 0b0000 - NUMERICAL 4bit (0-14 chars)
            if ( ( header &= 0x0F ) == 0 ) return decodeUTF8( data );
            //$FALL-THROUGH$
        case 1: // 0b0001 - NUMERICAL 4bit (15 chars)
            table = NUMERICAL;
            break;
        case 2: // 0b0010 - UPPER 5bit (0-11 chars)
            header &= 0x0F;
            //$FALL-THROUGH$
        case 3: // 0b0011 - UPPER 5bit (12 chars)
            table = UPPER;
            break;
        case 4: // 0b0100 - LOWER 5bit (0-11 chars)
            header &= 0x0F;
            //$FALL-THROUGH$
        case 5: // 0b0101 - LOWER 5bit (12 chars)
            table = LOWER;
            break;
        case 6: // 0b0110 - ALPHANUM 6bit (10 chars)
            table = ALPHANUM;
            break;
        case 7: // 0b0111 - EUROPEAN 7bit (1-8 chars) or LATIN1 8bit (0-7 chars)
            header &= 0x0F;
            if ( ( header & 0x08 ) != 0 )
            { // 0b0111 1 - LATIN1 8bit (0-7 chars)
                return decodeLatin1( data, ( header & 0x07 ) + 1 );
            }
            else
            { // 0b0111 0 - EUROPEAN 7bit (1-8 chars)
                header += 1; // offset char count
            }
            //$FALL-THROUGH$
        default: // 0b1XXX- EUROPEAN 7bit (9 chars)
            table = EUROPEAN;
            break;
        }
        if ( header > 15 ) header = table.max; // header is now length
        char[] result = new char[header];
        // encode shifts in the bytes with the first char at the MSB, therefore
        // we must "unshift" in the reverse order
        for ( int i = result.length - 1; i >= 0; i-- )
        {
            result[i] = table.decTranslate( (byte) ( data & table.mask ) );
            data >>>= table.step;
        }
        return new String( result );
    }

    private static boolean encodeLatin1( String string, PropertyRecord target )
    { // see doEncode
        long result = 0x78 | ( string.length() - 1 );
        result <<= ( (BYTES-1) - string.length() ) * 8; // move the header to its place
        for ( int i = 0; i < string.length(); i++ )
        {
            char c = string.charAt( i );
            if ( c < 0 || c >= 256 ) return false;
            result = ( result << 8 ) | c;
        }
        target.setPropBlock( result );
        return true;
    }

    private static boolean encodeUTF8( byte[] bytes, PropertyRecord target )
    { // UTF-8 padded with null bytes
        if ( bytes.length > BYTES-1 ) return false;
        long result = 0;
        for ( byte b : bytes )
        {
            result = ( result << 8 ) | ( 0xFF & b );
        }
        target.setPropBlock( result );
        return true;
    }

    private boolean doEncode( byte[] data, PropertyRecord target )
    {
        if ( data.length > max ) return false;
        long result = header( data.length );
        result <<= ( max - data.length ) * step; // move the header to its place
        for ( int i = 0; i < data.length; i++ )
        { // shift the data along and mask in each piece
            if ( i != 0 ) result <<= step;
            result |= encTranslate( data[i] );
        }
        target.setPropBlock( result );
        return true;
    }

    private static String decodeLatin1( long data, int length )
    { // see decode
        char[] result = new char[length];
        for ( int i = result.length - 1; i >= 0; i-- )
        {
            result[i] = (char) ( data & 0xFF );
            data >>>= 8;
        }
        return new String( result );
    }

    private static String decodeUTF8( long data )
    {
        byte[] temp = new byte[BYTES-1];
        int size = BYTES-1;
        while ( data != 0 ) // since we pad with null bytes
        {
            temp[--size] = (byte) ( data & 0xFF );
            data >>>= 8;
        }
        // we didn't know the length up front, compensate for that
        byte[] result;
        if ( size == 0 )
        {
            result = temp;
        }
        else
        {
            result = new byte[temp.length - size];
            for ( int i = 0; i < result.length; i++ )
            {
                result[i] = temp[size + i];
            }
        }
        try
        {
            return new String( result, "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new IllegalStateException( "All JVMs must support UTF-8", e );
        }
    }
}
