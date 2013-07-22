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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.UnsupportedEncodingException;
import java.util.EnumSet;

/**
 * Supports encoding alphanumerical and <code>SP . - + , ' : / _</code>
 *
 * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
 */
public enum ShortString
{
    /**
     * Binary coded decimal with punctuation.
     *
     * <pre>
     * HEADER (binary): 0000 LENG DATA... (0-14 chars) [4bit data]
     * HEADER (binary): 0001 DATA... (15 chars) [4bit data]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9  +  ,  ' SP  .  -
     * </pre>
     */
    NUMERICAL( 15, 0x0F, 4 )
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
            return decPunctuation( codePoint - 10 + 6 );
        }

        @Override
        long header( int length )
        {
            if ( length == max ) return 0x10;
            return length;
        }
    },
    /**
     * Upper-case characters with punctuation.
     *
     * <pre>
     * HEADER (binary): 0010 LENG DATA... (0-11 chars) [5bit data]
     * HEADER (binary): 0011 DATA... (12 chars) [5bit data]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  _  .  -  :  /
     * </pre>
     */
    UPPER( 12, 0x1F, 5 )
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
            if ( length == max ) return 0x30 << 1;
            return ( 0x20 | length ) << 1;
        }
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     * HEADER (binary): 0100 LENG DATA... (0-11 chars) [5bit data]
     * HEADER (binary): 0101 DATA... (12 chars) [5bit data]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  :  /
     * </pre>
     */
    LOWER( 12, 0x1F, 5 )
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
            if ( length == max ) return 0x50 << 1;
            return ( 0x40 | length ) << 1;
        }
    },
    /**
     * Alpha-numerical characters space and underscore.
     *
     * HEADER (binary): 0110 LENG DATA... (10 chars) [6bit data]
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  0  1  2  3  4
     * 2-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 3-  p  q  r  s  t  u  v  w    x  y  z  5  6  7  8  9
     * </pre>
     */
    ALPHANUM( 10, 0x3F, 6 )
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
            return 0x60 << 2;
        }
    },
    /**
     * The most common European characters (latin-1 but with less punctuation).
     *
     * <pre>
     * HEADER (binary): 0111 0LEN DATA... (1-8 chars) [7bit data]
     * HEADER (binary): 1DATA... (9 chars) [7bit data]
     *
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  À  Á  Â  Ã  Ä  Å  Æ  Ç    È  É  Ê  Ë  Ì  Í  Î  Ï
     * 1-  Ð  Ñ  Ò  Ó  Ô  Õ  Ö  .    Ø  Ù  Ú  Û  Ü  Ý  Þ  ß
     * 2-  à  á  â  ã  ä  å  æ  ç    è  é  ê  ë  ì  í  î  ï
     * 3-  ð  ñ  ò  ó  ô  õ  ö  -    ø  ù  ú  û  ü  ý  þ  ÿ
     * 4- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 5-  P  Q  R  S  T  U  V  W    X  Y  Z  0  1  2  3  4
     * 6-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 7-  p  q  r  s  t  u  v  w    x  y  z  5  6  7  8  9
     * </pre>
     */
    EUROPEAN( 9, 0x7F, 7 )
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
            default:
                throw cannotEncode( b );
            }
        }

        @Override
        long header( int length )
        {
            if ( length == max ) return 0x80;
            return 0x70 | ( length - 1 );
        }
    };

    final int max;
    final short mask;
    final short step;

    private ShortString( int max, int mask, int step )
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
        System.out.println( Long.toHexString( (byte) 'À' ) );
        System.out.println( Long.toHexString( (byte) 'ÿ' ) );
        System.out.println( Long.toHexString( EUROPEAN.encTranslate( (byte) 'À' ) ) );
        System.out.println( Long.toHexString( EUROPEAN.encTranslate( (byte) 'ÿ' ) ) );
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
    public static boolean encode( int keyId, String string, PropertyRecord target )
    {
        if ( string.length() > 15 ) return false; // Not handled by any encoding
        if ( string.equals( "" ) )
        {
            applyInRecord( target, keyId, 0 );
            return true;
        }
        // Keep track of the possible encodings that can be used for the string
        EnumSet<ShortString> possible = null;
        // First try encoding using Latin-1
        if ( string.length() < 8 )
        {
            if ( encodeLatin1( keyId, string, target ) ) return true;
            // If the string was short enough, but still didn't fit in latin-1
            // we know that no other encoding will work either, remember that
            // so that we can try UTF-8 at the end of this method
            possible = EnumSet.noneOf( ShortString.class );
        }
        // Allocate space for the intermediate representation
        // (using the intermediate representation table above)
        byte[] data = new byte[string.length()];
        if ( possible == null )
        {
            possible = EnumSet.allOf( ShortString.class );
            // ALPHANUM can only store len == 10
            if ( data.length != 10 ) possible.remove( ALPHANUM );
            if ( data.length > 9 ) possible.remove( EUROPEAN );
            if ( data.length > 12 ) possible.removeAll( EnumSet.of( UPPER, LOWER ) );
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
                possible.remove( NUMERICAL );
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
                possible.removeAll( EnumSet.of( ALPHANUM, NUMERICAL, EUROPEAN ) );
                break;
            case '/':
                data[i] = 5;
                possible.removeAll( EnumSet.of( ALPHANUM, NUMERICAL, EUROPEAN ) );
                break;
            case '+':
                data[i] = 6;
                possible.retainAll( EnumSet.of( NUMERICAL ) );
                break;
            case ',':
                data[i] = 7;
                possible.retainAll( EnumSet.of( NUMERICAL ) );
                break;
            case '\'':
                data[i] = 8;
                possible.retainAll( EnumSet.of( NUMERICAL ) );
                break;
            default:
                if ( c >= 'A' && c <= 'Z' )
                {
                    possible.remove( NUMERICAL );
                    possible.remove( LOWER );
                }
                else if ( c >= 'a' && c <= 'z' )
                {
                    possible.remove( NUMERICAL );
                    possible.remove( UPPER );
                }
                else if ( c >= '0' && c <= '9' )
                {
                    possible.remove( UPPER );
                    possible.remove( LOWER );
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
        for ( ShortString encoding : possible )
        {
            // Will return false if the data is too long for the encoding
            if ( encoding.doEncode( keyId, data, target ) ) return true;
        }
        if ( string.length() <= 6 )
        { // We might have a chance with UTF-8 - try it!
            try
            {
                return encodeUTF8( keyId, string.getBytes( "UTF-8" ), target );
            }
            catch ( UnsupportedEncodingException e )
            {
                throw new IllegalStateException( "All JVMs must support UTF-8", e );
            }
        }
        return false;
    }

    private static void applyInRecord( PropertyRecord target, int keyId, long propBlock )
    {
//        long data = 0;
//        data |= ( (long) keyId << 40 );
//        data |= ( (long) PropertyType.SHORT_STRING.intValue() << 36 );
//        data |= ( (long) encoding << 32 );
//        data |= ( (long) stringLength << 28 );
//
//        target.setSinglePropBlock( data );
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
        int header = (int) ( data >>> 56 );
        ShortString table;
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

    private static boolean encodeLatin1( int keyId, String string, PropertyRecord target )
    { // see doEncode
        long result = 0x78 | ( string.length() - 1 );
        result <<= ( 7 - string.length() ) * 8; // move the header to its place
        for ( int i = 0; i < string.length(); i++ )
        {
            char c = string.charAt( i );
            if ( c < 0 || c >= 256 ) return false;
            result = ( result << 8 ) | c;
        }
        applyInRecord( target, keyId, result );
        return true;
    }

    private static boolean encodeUTF8( int keyId, byte[] bytes, PropertyRecord target )
    { // UTF-8 padded with null bytes
        if ( bytes.length > 7 ) return false;
        long result = 0;
        for ( byte b : bytes )
        {
            result = ( result << 8 ) | ( 0xFF & b );
        }
        applyInRecord( target, keyId, result );
        return true;
    }

    private boolean doEncode( int keyId, byte[] data, PropertyRecord target )
    {
        if ( data.length > max ) return false;
        long result = header( data.length );
        result <<= ( max - data.length ) * step; // move the header to its place
        for ( int i = 0; i < data.length; i++ )
        { // shift the data along and mask in each piece
            if ( i != 0 ) result <<= step;
            result |= encTranslate( data[i] );
        }
        applyInRecord( target, keyId, result );
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
        byte[] temp = new byte[7];
        int size = 7;
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
            result = new byte[7 - size];
            System.arraycopy( temp, size, result, 0, result.length );
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
