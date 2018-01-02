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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

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
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9 SP  .  -  +  ,  '
     * </pre>
     */
    NUMERICAL( 1, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            switch ( b )
            {
            // interm.    encoded
            case 0: return 0xA;
            case 2: return 0xB;
            case 3: return 0xC;
            case 6: return 0xD;
            case 7: return 0xE;
            case 8: return 0xF;
            default: throw cannotEncode( b );
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
    },
    /**
     * Binary coded decimal with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9 SP  -  :  /  +  ,
     * </pre>
     */
    DATE( 2, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            switch ( b )
            {
            case 0: return 0xA;
            case 3: return 0xB;
            case 4: return 0xC;
            case 5: return 0xD;
            case 6: return 0xE;
            case 7: return 0xF;
            default: throw cannotEncode( b );
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
            if ( codePoint < 0xA ) return (char) ( codePoint + '0' );
            switch ( codePoint )
            {
            case 0xA: return ' ';
            case 0xB: return '-';
            case 0xC: return ':';
            case 0xD: return '/';
            case 0xE: return '+';
            default: return ',';
            }
        }
    },
    /**
     * Upper-case characters with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  _  .  -  :  /
     * </pre>
     */
    UPPER( 3, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate(b) - 0x40;
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
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  :  /
     * </pre>
     */
    LOWER( 4, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate(b) - 0x60;
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
    },
    /**
     * Lower-case characters with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  ,  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z  _  .  -  +  @
     * </pre>
     */
    EMAIL( 5, 5 )
    {
        @Override
        int encTranslate( byte b )
        {
            return super.encTranslate(b) - 0x60;
        }

        @Override
        int encPunctuation( byte b )
        {
            int encOffset = 0x60;
            if ( b == 7 ) return encOffset;

            int offset = encOffset + 0x1B;
            switch ( b )
            {
            case 1: return 0 + offset;
            case 2: return 1 + offset;
            case 3: return 2 + offset;
            case 6: return 3 + offset;
            case 9: return 4 + offset;
            default: throw cannotEncode( b );
            }
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ',';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            switch ( codePoint )
            {
            case 0x1E: return '+';
            case 0x1F: return '@';
            default: return decPunctuation( codePoint - 0x1A );
            }
        }
    },
    /**
     * Lower-case characters, digits and punctuation and symbols.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 1-  p  q  r  s  t  u  v  w    x  y  z
     * 2-  0  1  2  3  4  5  6  7    8  9  _  .  -  :  /  +
     * 3-  ,  '  @  |  ;  *  ?  &    %  #  (  )  $  <  >  =
     * </pre>
     */
    URI( 6, 6 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b == 0 ) return 0; // space
            if ( b >= 0x61 && b <= 0x7A ) return b - 0x60; // lower-case letters
            if ( b >= 0x30 && b <= 0x39 ) return b - 0x10; // digits
            if ( b >= 0x1 && b <= 0x16 ) return b + 0x29; // symbols
            throw cannotEncode( b );
        }

        @Override
        int encPunctuation( byte b )
        {
            // Handled by encTranslate
            throw cannotEncode( b );
        }

        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char) ( codePoint + 'a' - 1 );
            if ( codePoint <= 0x29 ) return (char) (codePoint - 0x20 + '0');
            if ( codePoint <= 0x2E ) return decPunctuation( codePoint - 0x29 );
            return decPunctuation( codePoint - 0x2F + 9);
        }
    },
    /**
     * Alpha-numerical characters space and underscore.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  0  1  2  3  4
     * 2-  _  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 3-  p  q  r  s  t  u  v  w    x  y  z  5  6  7  8  9
     * </pre>
     */
    ALPHANUM( 7, 6 )
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
    },
    /**
     * Alpha-numerical characters space and underscore.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0- SP  A  B  C  D  E  F  G    H  I  J  K  L  M  N  O
     * 1-  P  Q  R  S  T  U  V  W    X  Y  Z  _  .  -  :  /
     * 2-  ;  a  b  c  d  e  f  g    h  i  j  k  l  m  n  o
     * 3-  p  q  r  s  t  u  v  w    x  y  z  +  ,  '  @  |
     * </pre>
     */
    ALPHASYM( 8, 6 )
    {
        @Override
        char decTranslate( byte codePoint )
        {
            if ( codePoint == 0x0 ) return ' ';
            if ( codePoint <= 0x1A ) return (char)('A' + codePoint - 0x1);
            if ( codePoint <= 0x1F ) return decPunctuation( codePoint - 0x1B + 1 );
            if ( codePoint == 0x20 ) return ';';
            if ( codePoint <= 0x3A ) return (char)('a' + codePoint - 0x21);
            return decPunctuation( codePoint - 0x3B + 9 );
        }

        @Override
        int encTranslate( byte b )
        {
            // Punctuation is in the same places as European
            if ( b < 0x20 ) return encPunctuation( b ); // Punctuation
            // But the rest is transposed by 0x40
//            return EUROPEAN.encTranslate( b ) - 0x40;
            return b - 0x40;
        }

        @Override
        int encPunctuation( byte b )
        {
            switch ( b )
            {
            case 0x0: return 0x0;
            case 0x1: return 0x1B;
            case 0x2: return 0x1C;
            case 0x3: return 0x1D;
            case 0x4: return 0x1E;
            case 0x5: return 0x1F;

            case 0x6: return 0x3B;
            case 0x7: return 0x3C;
            case 0x8: return 0x3D;
            case 0x9: return 0x3E;
            case 0xA: return 0x3F;

            case 0xB: return 0x20;
            default: throw cannotEncode( b );
            }
        }
    },
    /**
     * The most common European characters (latin-1 but with less punctuation).
     *
     * <pre>
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
    EUROPEAN( 9, 7 )
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
    },
    // ENCODING_LATIN1 is 10th
    /**
     * Lower-case characters a-f and digits.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7 -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f
     * </pre>
     */
    LOWERHEX( 11, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            if ( b >= 'a' && b <= 'f' ) return b - 'a' + 10;
            throw cannotEncode( b );
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
            return (char) ( codePoint + 'a' - 10 );
        }
    },
    /**
     * Upper-case characters A-F and digits.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7 -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
     * </pre>
     */
    UPPERHEX( 12, 4 )
    {
        @Override
        int encTranslate( byte b )
        {
            if ( b >= '0' && b <= '9' ) return b - '0';
            if ( b >= 'A' && b <= 'F' ) return b - 'A' + 10;
            throw cannotEncode( b );
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
            return (char) ( codePoint + 'A' - 10 );
        }
    };
    public static final int REMOVE_LARGE_ENCODINGS_MASK = invertedBitMask( ALPHANUM, ALPHASYM, URI, EUROPEAN );
    public static final LongerShortString[] ENCODINGS = values();
    public static final int ENCODING_COUNT = ENCODINGS.length;
    public static final int ALL_BIT_MASK = bitMask( LongerShortString.values() );
    public static final int ENCODING_UTF8 = 0;
    public static final int ENCODING_LATIN1 = 10;
    private static final int HEADER_SIZE = 39; // bits

    final int encodingHeader;
    final long mask;
    final int step;

    private LongerShortString( int encodingHeader, int step )
    {
        this.encodingHeader = encodingHeader;
        this.mask = Bits.rightOverflowMask( step );
        this.step = step;
    }

    int maxLength( int payloadSize )
    {
        // key-type-encoding-length
        return ((payloadSize << 3)-24-4-4-6)/step;
    }

    final IllegalArgumentException cannotEncode( byte b )
    {
        return new IllegalArgumentException( "Cannot encode as " + this.name() + ": " + b );
    }

    /** Lookup table for decoding punctuation */
    private static final char[] PUNCTUATION = {
        ' ', '_', '.', '-', ':', '/',
        ' ', '.', '-', '+', ',', '\'', '@', '|', ';', '*', '?', '&', '%', '#', '(', ')', '$', '<', '>', '=' };

    final char decPunctuation( int code )
    {
        return PUNCTUATION[code];
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
     * 0- SP  _  .  -  :  /  +  ,    '  @  |  ;  *  ?  &  %
     * 1-  #  (  )  $  <  >  =
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
    public static boolean encode( int keyId, String string,
                                  PropertyBlock target, int payloadSize )
    {
        // NUMERICAL can carry most characters, so compare to that
        int dataLength = string.length();
        // We only use 6 bits for storing the string length
        // TODO could be dealt with by having string length zero and go for null bytes,
        // at least for LATIN1 (that's what the ShortString implementation initially did)
        if ( dataLength > NUMERICAL.maxLength( payloadSize ) || dataLength > 63 )
        {
            return false; // Not handled by any encoding
        }

        // Allocate space for the intermediate representation
        // (using the intermediate representation table above)
        byte[] data = new byte[dataLength];

        // Keep track of the possible encodings that can be used for the string
        // 0 means none applies
        int encodings = determineEncoding( string, data, dataLength, payloadSize );
        if ( encodings != 0 && tryEncode( encodings, keyId, target, payloadSize, data, dataLength ) )
        {
            return true;
        }
        return encodeWithCharSet( keyId, string, target, payloadSize, dataLength );
    }

    private static boolean encodeWithCharSet(int keyId, String string, PropertyBlock target, int payloadSize, int stringLength)
    {
        int maxBytes = PropertyType.getPayloadSize();
        if ( stringLength <= maxBytes - 5 )
        {
            if ( encodeLatin1( keyId, string, target ) ) return true;
            if ( encodeUTF8( keyId, string, target, payloadSize ) ) return true;
        }
        return false;
    }

    private static boolean tryEncode( int encodings, int keyId, PropertyBlock target, int payloadSize, byte[] data, final int length )
    {
        // find encoders in order that are still selected and try to encode the data
        for ( LongerShortString encoding : ENCODINGS )
        {
            if ( (encoding.bitMask() & encodings) == 0 )
            {
                continue;
            }
            if ( encoding.doEncode( keyId, data, target, payloadSize, length ) )
            {
                return true;
            }
        }
        return false;
    }

    // inverted combined bit-mask for the encoders
    static int invertedBitMask( LongerShortString... encoders )
    {
        return ~bitMask( encoders );
    }

    // combined bit-mask for the encoders
    private static int bitMask( LongerShortString[] encoders )
    {
        int result = 0;
        for ( LongerShortString encoder : encoders )
        {
            result |= encoder.bitMask();
        }
        return result;
    }

    // translation lookup for each ascii character
    private static final int TRANSLATION_COUNT = 256;
    // transformation for the char to byte according to the default translation table
    private static final byte[] TRANSLATION = new byte[TRANSLATION_COUNT];
    // mask for encoders that are not applicable for this character
    private static final int[] REMOVE_MASK = new int[TRANSLATION_COUNT];

    private static void setUp( char pos, int value, LongerShortString... removeEncodings )
    {
        TRANSLATION[pos] = (byte) value;
        REMOVE_MASK[pos] = invertedBitMask( removeEncodings );
    }

    static
    {
        Arrays.fill( TRANSLATION, (byte) 0xFF );
        Arrays.fill( REMOVE_MASK, invertedBitMask( ENCODINGS ) );
        setUp( ' ', 0, EMAIL, LOWERHEX, UPPERHEX );
        setUp( '_', 1, NUMERICAL, DATE, LOWERHEX, UPPERHEX );
        setUp( '.', 2, DATE, ALPHANUM, LOWERHEX, UPPERHEX );
        setUp( '-', 3, ALPHANUM, LOWERHEX, UPPERHEX );
        setUp( ':', 4, ALPHANUM, NUMERICAL, EUROPEAN, EMAIL, LOWERHEX, UPPERHEX );
        setUp( '/', 5, ALPHANUM, NUMERICAL, EUROPEAN, EMAIL, LOWERHEX, UPPERHEX );
        setUp( '+', 6, UPPER, LOWER, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX );
        setUp( ',', 7, UPPER, LOWER, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX );
        setUp( '\'', 8, DATE, UPPER, LOWER, EMAIL, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX );
        setUp( '@', 9, NUMERICAL, DATE, UPPER, LOWER, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX );
        setUp( '|', 0xA, NUMERICAL, DATE, UPPER, LOWER, EMAIL, URI, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX );
        final LongerShortString[] retainUri = {NUMERICAL, DATE, UPPER, LOWER, EMAIL, ALPHANUM, ALPHASYM, EUROPEAN, LOWERHEX, UPPERHEX};
        setUp( ';', 0xB, retainUri );
        setUp( '*', 0xC, retainUri );
        setUp( '?', 0xD, retainUri );
        setUp( '&', 0xE, retainUri );
        setUp( '%', 0xF, retainUri );
        setUp( '#', 0x10, retainUri );
        setUp( '(', 0x11, retainUri );
        setUp( ')', 0x12, retainUri );
        setUp( '$', 0x13, retainUri );
        setUp( '<', 0x14, retainUri );
        setUp( '>', 0x15, retainUri );
        setUp( '=', 0x16, retainUri );
        for ( char c = 'A'; c <= 'F'; c++ )
        {
            setUp( c, (byte) c, NUMERICAL, DATE, LOWER, EMAIL, URI, LOWERHEX );
        }
        for ( char c = 'G'; c <= 'Z'; c++ )
        {
            setUp( c, (byte) c, NUMERICAL, DATE, LOWER, EMAIL, URI, LOWERHEX, UPPERHEX );
        }
        for ( char c = 'a'; c <= 'f'; c++ )
        {
            setUp( c, (byte) c, NUMERICAL, DATE, UPPER, UPPERHEX );
        }
        for ( char c = 'g'; c <= 'z'; c++ )
        {
            setUp( c, (byte) c, NUMERICAL, DATE, UPPER, UPPERHEX, LOWERHEX );
        }
        for ( char c = '0'; c <= '9'; c++ )
        {
            setUp( c, (byte) c, UPPER, LOWER, EMAIL, ALPHASYM );
        }
        for ( char c = 'À'; c <= 'ÿ'; c++ )
        {
            if ( c != 0xD7 && c != 0xF7 )
            {
                setUp( c, (byte) c, NUMERICAL, DATE, UPPER, LOWER, EMAIL, URI, ALPHANUM, ALPHASYM, LOWERHEX, UPPERHEX );
            }
        }
    }

    private static int determineEncoding( String string, byte[] data, int length, int payloadSize )
    {
        if ( length == 0 )
        {
            return 0;
        }
        int encodings = ALL_BIT_MASK;
        // filter out larger encodings in one go
        if ( length > ALPHANUM.maxLength( payloadSize ) )
        {
            encodings &= REMOVE_LARGE_ENCODINGS_MASK;
        }
        for ( int i = 0; i < length; i++ )
        {
            char c = string.charAt( i );
            // non ASCII chars not supported
            if ( c >= TRANSLATION_COUNT )
            {
                return 0;
            }
            data[i] = TRANSLATION[c];
            // remove not matching encoders
            encodings &= REMOVE_MASK[c];
            if ( encodings == 0 )
            {
                return 0;
            }
        }
        return encodings;
    }

    int bitMask()
    {
        return 1 << ordinal();
    }

    private static void writeHeader( Bits bits, int keyId, int encoding, int stringLength )
    {
        // [][][][ lll,llle][eeee,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        bits.put( keyId, 24 ).put( PropertyType.SHORT_STRING.intValue(), 4 ).put( encoding, 5 ).put( stringLength, 6 );
    }

    /**
     * Decode a short string represented as a long[]
     *
     * @param block the value to decode to a short string.
     * @return the decoded short string
     */
    public static String decode( PropertyBlock block )
    {
        return decode( block.getValueBlocks(), 0, block.getValueBlocks().length );
    }

    public static String decode( long[] blocks, int offset, int length )
    {
        long firstLong = blocks[offset];
        if ( ( firstLong & 0xFFFFFF0FFFFFFFFFL ) == 0 ) return "";
        // key(24b) + type(4) = 28
        int encoding = (int) ((firstLong & 0x1F0000000L) >>> 28); // 5 bits of encoding
        int stringLength = (int) ((firstLong & 0x7E00000000L) >>> 33); // 6 bits of stringLength
        if ( encoding == LongerShortString.ENCODING_UTF8 ) return decodeUTF8( blocks, offset, stringLength );
        if ( encoding == ENCODING_LATIN1 ) return decodeLatin1( blocks, offset, stringLength );

        LongerShortString table = getEncodingTable( encoding );
        assert table != null: "We only decode LongerShortStrings after we have consistently read the PropertyBlock " +
                              "data from the page cache. Thus, we should never have an invalid encoding header here.";
        char[] result = new char[stringLength];
        // encode shifts in the bytes with the first char at the MSB, therefore
        // we must "unshift" in the reverse order
        decode( result, blocks, offset, table );

        // We know the char array is unshared, so use sharing constructor explicitly
        return UnsafeUtil.newSharedArrayString( result );
    }

    private static void decode( char[] result, long[] blocks, int offset, LongerShortString table )
    {
        int block = offset;
        int maskShift = HEADER_SIZE;
        long baseMask = table.mask;
        for ( int i = 0; i < result.length; i++ )
        {
            byte codePoint = (byte) ((blocks[block] >>> maskShift) & baseMask);
            maskShift += table.step;
            if ( maskShift >= 64 && block + 1 < blocks.length )
            {
                maskShift %= 64;
                codePoint |= (blocks[++block] & (baseMask >>> (table.step-maskShift))) << (table.step-maskShift);
            }
            result[i] = table.decTranslate( codePoint );
        }
    }

    // lookup table by encoding header
    // +2 because of ENCODING_LATIN1 gap and one based index
    private final static LongerShortString[] ENCODINGS_BY_ENCODING = new LongerShortString[ENCODING_COUNT + 2];

    static
    {
        for ( LongerShortString encoding : ENCODINGS )
        {
            ENCODINGS_BY_ENCODING[encoding.encodingHeader] = encoding;
        }
    }

    /**
     * Get encoding table for the given encoding header, or {@code null} if the encoding header is invalid.
     */
    private static LongerShortString getEncodingTable( int encodingHeader )
    {
        if ( encodingHeader < 0 | ENCODINGS_BY_ENCODING.length <= encodingHeader )
        {
            return null;
        }
        return ENCODINGS_BY_ENCODING[encodingHeader];
    }

    private static Bits newBits( LongerShortString encoding, int length )
    {
        return Bits.bits(calculateNumberOfBlocksUsed( encoding, length ) << 3 ); //*8
    }

    private static Bits newBitsForStep8(int length)
    {
        return Bits.bits(calculateNumberOfBlocksUsedForStep8(length) << 3 ); //*8
    }

    private static boolean encodeLatin1( int keyId, String string, PropertyBlock target )
    {
        int length = string.length();
        Bits bits = newBitsForStep8(length);
        writeHeader( bits, keyId, ENCODING_LATIN1, length );
        if ( !writeLatin1Characters( string, bits ) ) return false;
        target.setValueBlocks( bits.getLongs() );
        return true;
    }

    public static boolean writeLatin1Characters( String string, Bits bits )
    {
        int length = string.length();
        for ( int i = 0; i < length; i++ )
        {
            char c = string.charAt( i );
            if ( c >= 256 )
            {
                return false;
            }
            bits.put( c, 8 ); // Just the lower byte
        }
        return true;
    }

    private static boolean encodeUTF8( int keyId, String string,
            PropertyBlock target, int payloadSize )
    {
        try
        {
            byte[] bytes = string.getBytes( "UTF-8" );
            final int length = bytes.length;
            if ( length > payloadSize-3/*key*/-2/*enc+len*/ ) return false;
            Bits bits = newBitsForStep8(length);
            writeHeader( bits, keyId, ENCODING_UTF8, length); // In this case it isn't the string length, but the number of bytes
            for ( byte value : bytes )
            {
                bits.put( value );
            }
            target.setValueBlocks( bits.getLongs() );
            return true;
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new IllegalStateException( "All JVMs must support UTF-8", e );
        }
    }

    private boolean doEncode(int keyId, byte[] data, PropertyBlock target,
                             int payloadSize, final int length)
    {
        if ( length > maxLength( payloadSize ) ) return false;
        Bits bits = newBits( this, length);
        writeHeader( bits, keyId, encodingHeader, length);
        if (length >0) translateData(bits, data, length, step);
        target.setValueBlocks( bits.getLongs() );
        return true;
    }

    private void translateData(Bits bits, byte[] data, int length, final int step)
    {
        for (int i = 0; i < length; i++)
        {
            bits.put(encTranslate(data[i]), step);
        }
    }

    private static String decodeLatin1( long[] blocks, int offset, int stringLength )
    {
        char[] result = new char[stringLength];
        int block = offset;
        int maskShift = HEADER_SIZE;
        for ( int i = 0; i < result.length; i++ )
        {
            char codePoint = (char) ((blocks[block] >>> maskShift) & 0xFF);
            maskShift += 8;
            if ( maskShift >= 64 )
            {
                maskShift %= 64;
                codePoint |= (blocks[++block] & (0xFF >>> (8-maskShift))) << (8-maskShift);
            }
            result[i] = codePoint;
        }
        return UnsafeUtil.newSharedArrayString( result );
    }

    private static String decodeUTF8( long[] blocks, int offset, int stringLength )
    {
        byte[] result = new byte[stringLength];
        int block = offset;
        int maskShift = HEADER_SIZE;
        for ( int i = 0; i < result.length; i++ )
        {
            byte codePoint = (byte) (blocks[block] >>> maskShift);
            maskShift += 8;
            if ( maskShift >= 64 )
            {
                maskShift %= 64;
                codePoint |= (blocks[++block] & (0xFF >>> (8-maskShift))) << (8-maskShift);
            }
            result[i] = codePoint;
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

    public static int calculateNumberOfBlocksUsed( long firstBlock )
    {
        /*
         * [ lll,llle][eeee,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
         */
        int encoding = (int) ( ( firstBlock & 0x1F0000000L ) >> 28 );
        int length = (int) ( ( firstBlock & 0x7E00000000L ) >> 33 );
        if ( encoding == ENCODING_UTF8 || encoding == ENCODING_LATIN1 )
        {
            return calculateNumberOfBlocksUsedForStep8(length);
        }

        LongerShortString encodingTable = getEncodingTable( encoding );
        if ( encodingTable == null )
        {
            // We probably did an inconsistent read of the first block
            return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
        }
        return calculateNumberOfBlocksUsed( encodingTable, length );
    }

    public static int calculateNumberOfBlocksUsedForStep8( int length )
    {
        return totalBits(length << 3); // * 8
    }
    public static int calculateNumberOfBlocksUsed( LongerShortString encoding, int length )
    {
        return totalBits(length * encoding.step);
    }

    private static int totalBits( int bitsForCharacters )
    {
        int bitsInTotal = 24 + 4 + 5 + 6 + bitsForCharacters;
        return ((bitsInTotal - 1) >> 6) + 1; // /64
    }
}
