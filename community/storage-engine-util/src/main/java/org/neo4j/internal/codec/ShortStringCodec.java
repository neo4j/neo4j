/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.codec;

import java.util.Arrays;
import org.neo4j.util.BitBuffer;

public enum ShortStringCodec {
    /**
     * Binary coded decimal with punctuation.
     *
     * <pre>
     *    -0 -1 -2 -3 -4 -5 -6 -7   -8 -9 -A -B -C -D -E -F
     * 0-  0  1  2  3  4  5  6  7    8  9 SP  .  -  +  ,  '
     * </pre>
     */
    NUMERICAL(1, 4, false) {
        @Override
        public int encTranslate(byte b) {
            if (b >= '0' && b <= '9') {
                return b - '0';
            }
            return switch (b) {
                    // interm.    encoded
                case 0 -> 0xA;
                case 2 -> 0xB;
                case 3 -> 0xC;
                case 6 -> 0xD;
                case 7 -> 0xE;
                case 8 -> 0xF;
                default -> throw cannotEncode(b);
            };
        }

        @Override
        int encPunctuation(byte b) {
            throw cannotEncode(b);
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint < 10) {
                return (byte) (codePoint + '0');
            }
            return decPunctuation(codePoint - 10 + 6);
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
    DATE(2, 4, false) {
        @Override
        public int encTranslate(byte b) {
            if (b >= '0' && b <= '9') {
                return b - '0';
            }
            return switch (b) {
                case 0 -> 0xA;
                case 3 -> 0xB;
                case 4 -> 0xC;
                case 5 -> 0xD;
                case 6 -> 0xE;
                case 7 -> 0xF;
                default -> throw cannotEncode(b);
            };
        }

        @Override
        int encPunctuation(byte b) {
            throw cannotEncode(b);
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint < 0xA) {
                return (byte) (codePoint + '0');
            }
            return switch (codePoint) {
                case 0xA -> ' ';
                case 0xB -> '-';
                case 0xC -> ':';
                case 0xD -> '/';
                case 0xE -> '+';
                default -> ',';
            };
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
    UPPER(3, 5, false) {
        @Override
        public int encTranslate(byte b) {
            return super.encTranslate(b) - 0x40;
        }

        @Override
        int encPunctuation(byte b) {
            return b == 0 ? 0x40 : b + 0x5a;
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint == 0) {
                return ' ';
            }
            if (codePoint <= 0x1A) {
                return (byte) (codePoint + 'A' - 1);
            }
            return decPunctuation(codePoint - 0x1A);
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
    LOWER(4, 5, false) {
        @Override
        public int encTranslate(byte b) {
            return super.encTranslate(b) - 0x60;
        }

        @Override
        int encPunctuation(byte b) {
            return b == 0 ? 0x60 : b + 0x7a;
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint == 0) {
                return ' ';
            }
            if (codePoint <= 0x1A) {
                return (byte) (codePoint + 'a' - 1);
            }
            return decPunctuation(codePoint - 0x1A);
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
    EMAIL(5, 5, false) {
        @Override
        public int encTranslate(byte b) {
            return super.encTranslate(b) - 0x60;
        }

        @Override
        int encPunctuation(byte b) {
            int encOffset = 0x60;
            if (b == 7) {
                return encOffset;
            }

            int offset = encOffset + 0x1B;
            return switch (b) {
                case 1 -> offset;
                case 2 -> 1 + offset;
                case 3 -> 2 + offset;
                case 6 -> 3 + offset;
                case 9 -> 4 + offset;
                default -> throw cannotEncode(b);
            };
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint == 0) {
                return ',';
            }
            if (codePoint <= 0x1A) {
                return (byte) (codePoint + 'a' - 1);
            }
            return switch (codePoint) {
                case 0x1E -> '+';
                case 0x1F -> '@';
                default -> decPunctuation(codePoint - 0x1A);
            };
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
    URI(6, 6, false) {
        @Override
        public int encTranslate(byte b) {
            if (b == 0) {
                return 0; // space
            }
            if (b >= 0x61 && b <= 0x7A) {
                return b - 0x60; // lower-case letters
            }
            if (b >= 0x30 && b <= 0x39) {
                return b - 0x10; // digits
            }
            if (b >= 0x1 && b <= 0x16) {
                return b + 0x29; // symbols
            }
            throw cannotEncode(b);
        }

        @Override
        int encPunctuation(byte b) {
            // Handled by encTranslate
            throw cannotEncode(b);
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint == 0) {
                return ' ';
            }
            if (codePoint <= 0x1A) {
                return (byte) (codePoint + 'a' - 1);
            }
            if (codePoint <= 0x29) {
                return (byte) (codePoint - 0x20 + '0');
            }
            if (codePoint <= 0x2E) {
                return decPunctuation(codePoint - 0x29);
            }
            return decPunctuation(codePoint - 0x2F + 9);
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
    ALPHANUM(7, 6, false) {
        @Override
        public byte decTranslate(byte codePoint) {
            return EUROPEAN.decTranslate((byte) (codePoint + 0x40));
        }

        @Override
        public int encTranslate(byte b) {
            // Punctuation is in the same places as European
            if (b < 0x20) {
                return encPunctuation(b); // Punctuation
            }
            // But the rest is transposed by 0x40
            return EUROPEAN.encTranslate(b) - 0x40;
        }

        @Override
        int encPunctuation(byte b) {
            return switch (b) {
                case 0 -> 0x00; // SPACE
                case 1 -> 0x20; // UNDERSCORE
                default -> throw cannotEncode(b);
            };
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
    ALPHASYM(8, 6, false) {
        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint == 0x0) {
                return ' ';
            }
            if (codePoint <= 0x1A) {
                return (byte) ('A' + codePoint - 0x1);
            }
            if (codePoint <= 0x1F) {
                return decPunctuation(codePoint - 0x1B + 1);
            }
            if (codePoint == 0x20) {
                return ';';
            }
            if (codePoint <= 0x3A) {
                return (byte) ('a' + codePoint - 0x21);
            }
            return decPunctuation(codePoint - 0x3B + 9);
        }

        @Override
        public int encTranslate(byte b) {
            // Punctuation is in the same places as European
            if (b < 0x20) {
                return encPunctuation(b); // Punctuation
            }
            // But the rest is transposed by 0x40
            //            return EUROPEAN.encTranslate( b ) - 0x40;
            return b - 0x40;
        }

        @Override
        int encPunctuation(byte b) {
            return switch (b) {
                case 0x0 -> 0x0;
                case 0x1 -> 0x1B;
                case 0x2 -> 0x1C;
                case 0x3 -> 0x1D;
                case 0x4 -> 0x1E;
                case 0x5 -> 0x1F;
                case 0x6 -> 0x3B;
                case 0x7 -> 0x3C;
                case 0x8 -> 0x3D;
                case 0x9 -> 0x3E;
                case 0xA -> 0x3F;
                case 0xB -> 0x20;
                default -> throw cannotEncode(b);
            };
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
    EUROPEAN(9, 7, true) {
        @Override
        public byte decTranslate(byte codePoint) {
            int code = codePoint & 0xFF;
            if (code < 0x40) {
                if (code == 0x17) {
                    return '.';
                }
                if (code == 0x37) {
                    return '-';
                }
                return (byte) (code + 0xC0);
            } else {
                if (code == 0x40) {
                    return ' ';
                }
                if (code == 0x60) {
                    return '_';
                }
                if (code >= 0x5B && code < 0x60) {
                    return (byte) ('0' + code - 0x5B);
                }
                if (code >= 0x7B && code < 0x80) {
                    return (byte) ('5' + code - 0x7B);
                }
                return (byte) code;
            }
        }

        @Override
        int encPunctuation(byte b) {
            return switch (b) {
                case 0x00 -> 0x40; // SPACE
                case 0x01 -> 0x60; // UNDERSCORE
                case 0x02 -> 0x17; // DOT
                case 0x03 -> 0x37; // DASH
                    // TODO
                case 0x07 -> 0;
                default -> throw cannotEncode(b);
            };
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
    LOWERHEX(11, 4, false) {
        @Override
        public int encTranslate(byte b) {
            if (b >= '0' && b <= '9') {
                return b - '0';
            }
            if (b >= 'a' && b <= 'f') {
                return b - 'a' + 10;
            }
            throw cannotEncode(b);
        }

        @Override
        int encPunctuation(byte b) {
            throw cannotEncode(b);
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint < 10) {
                return (byte) (codePoint + '0');
            }
            return (byte) (codePoint + 'a' - 10);
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
    UPPERHEX(12, 4, false) {
        @Override
        public int encTranslate(byte b) {
            if (b >= '0' && b <= '9') {
                return b - '0';
            }
            if (b >= 'A' && b <= 'F') {
                return b - 'A' + 10;
            }
            throw cannotEncode(b);
        }

        @Override
        int encPunctuation(byte b) {
            throw cannotEncode(b);
        }

        @Override
        public byte decTranslate(byte codePoint) {
            if (codePoint < 10) {
                return (byte) (codePoint + '0');
            }
            return (byte) (codePoint + 'A' - 10);
        }
    };

    public static final ShortStringCodec[] CODECS = values();
    public static final int CODEC_COUNT = CODECS.length;
    public static final int ALL_BIT_MASK = bitMask(CODECS);
    public static final int ENCODING_UTF8 = 0;
    public static final int ENCODING_LATIN1 = 10;
    /** Lookup table for decoding punctuation */
    private static final byte[] PUNCTUATION = {
        ' ', '_', '.', '-', ':', '/', ' ', '.', '-', '+', ',', '\'', '@', '|', ';', '*', '?', '&', '%', '#', '(', ')',
        '$', '<', '>', '='
    };
    // translation lookup for each ascii character
    private static final int TRANSLATION_COUNT = 256;
    // transformation for the char to byte according to the default translation table
    private static final byte[] TRANSLATION = new byte[TRANSLATION_COUNT];
    // mask for encoders that are not applicable for this character
    private static final int[] REMOVE_MASK = new int[TRANSLATION_COUNT];
    // lookup table by encoding header
    // +2 because of ENCODING_LATIN1 gap and one based index
    private static final ShortStringCodec[] CODEC_BY_ID = new ShortStringCodec[CODEC_COUNT + 2];
    // Why are LOWERHEX/UPPERHEX special? Originally these didn't exist and all in existence are ordered
    // such that the lower the codec ID, the smaller bits-per-character they had. Codec selection is done
    // by selecting the compatible codec with the smallest codec ID.
    // These two were added while keeping backwards-compatibility and are therefore out-of-order so needs
    // additional logic to be selected.
    private static final int HEX_CODECS_COMPATIBILITY_MASK = LOWERHEX.bitMask() | UPPERHEX.bitMask();
    private static final int HEX_CODECS_FILTER;

    private static void setUp(char pos, int value, ShortStringCodec... removeCodecs) {
        TRANSLATION[pos] = (byte) value;
        REMOVE_MASK[pos] = ~bitMask(removeCodecs);
        for (ShortStringCodec codec : CODECS) {
            CODEC_BY_ID[codec.codecId] = codec;
        }
    }

    static {
        Arrays.fill(TRANSLATION, (byte) 0xFF);
        Arrays.fill(REMOVE_MASK, ~bitMask(CODECS));
        setUp(' ', 0, EMAIL, LOWERHEX, UPPERHEX);
        setUp('_', 1, NUMERICAL, DATE, LOWERHEX, UPPERHEX);
        setUp('.', 2, DATE, ALPHANUM, LOWERHEX, UPPERHEX);
        setUp('-', 3, ALPHANUM, LOWERHEX, UPPERHEX);
        setUp(':', 4, ALPHANUM, NUMERICAL, EUROPEAN, EMAIL, LOWERHEX, UPPERHEX);
        setUp('/', 5, ALPHANUM, NUMERICAL, EUROPEAN, EMAIL, LOWERHEX, UPPERHEX);
        setUp('+', 6, UPPER, LOWER, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX);
        setUp(',', 7, UPPER, LOWER, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX);
        setUp('\'', 8, DATE, UPPER, LOWER, EMAIL, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX);
        setUp('@', 9, NUMERICAL, DATE, UPPER, LOWER, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX);
        setUp('|', 0xA, NUMERICAL, DATE, UPPER, LOWER, EMAIL, URI, ALPHANUM, EUROPEAN, LOWERHEX, UPPERHEX);
        final ShortStringCodec[] retainUri = {
            NUMERICAL, DATE, UPPER, LOWER, EMAIL, ALPHANUM, ALPHASYM, EUROPEAN, LOWERHEX, UPPERHEX
        };
        setUp(';', 0xB, retainUri);
        setUp('*', 0xC, retainUri);
        setUp('?', 0xD, retainUri);
        setUp('&', 0xE, retainUri);
        setUp('%', 0xF, retainUri);
        setUp('#', 0x10, retainUri);
        setUp('(', 0x11, retainUri);
        setUp(')', 0x12, retainUri);
        setUp('$', 0x13, retainUri);
        setUp('<', 0x14, retainUri);
        setUp('>', 0x15, retainUri);
        setUp('=', 0x16, retainUri);
        for (char c = 'A'; c <= 'F'; c++) {
            setUp(c, (byte) c, NUMERICAL, DATE, LOWER, EMAIL, URI, LOWERHEX);
        }
        for (char c = 'G'; c <= 'Z'; c++) {
            setUp(c, (byte) c, NUMERICAL, DATE, LOWER, EMAIL, URI, LOWERHEX, UPPERHEX);
        }
        for (char c = 'a'; c <= 'f'; c++) {
            setUp(c, (byte) c, NUMERICAL, DATE, UPPER, UPPERHEX);
        }
        for (char c = 'g'; c <= 'z'; c++) {
            setUp(c, (byte) c, NUMERICAL, DATE, UPPER, UPPERHEX, LOWERHEX);
        }
        for (char c = '0'; c <= '9'; c++) {
            setUp(c, (byte) c, UPPER, LOWER, EMAIL, ALPHASYM);
        }
        for (char c = 'À'; c <= 'ÿ'; c++) {
            if (c != 0xD7 && c != 0xF7) {
                setUp(c, (byte) c, NUMERICAL, DATE, UPPER, LOWER, EMAIL, URI, ALPHANUM, ALPHASYM, LOWERHEX, UPPERHEX);
            }
        }

        int hexCodecsFilter = 0;
        for (var codec : CODECS) {
            if (codec.step <= LOWERHEX.step) {
                hexCodecsFilter |= codec.bitMask();
            }
        }
        HEX_CODECS_FILTER = hexCodecsFilter;
    }

    final int codecId;
    final long mask;
    final int step;
    final boolean needsChars;
    private final int bitMask;

    ShortStringCodec(int codecId, int step, boolean needsChars) {
        this.codecId = codecId;
        this.mask = BitBuffer.rightOverflowMask(step);
        this.step = step;
        this.needsChars = needsChars;
        this.bitMask = (1 << (codecId - 1));
    }

    public static ShortStringCodec lowestCodec(int compatibleCodecs) {
        return codecById(Integer.numberOfTrailingZeros(compatibleCodecs) + 1);
    }

    public int bitMask() {
        return bitMask;
    }

    public int bitsPerCharacter() {
        return step;
    }

    public boolean needsChars() {
        return needsChars;
    }

    public long mask() {
        return mask;
    }

    public int id() {
        return codecId;
    }

    // combined bit-mask for the codecs
    public static int bitMask(ShortStringCodec... codecs) {
        int result = 0;
        for (ShortStringCodec codec : codecs) {
            result |= codec.bitMask();
        }
        return result;
    }

    final IllegalArgumentException cannotEncode(byte b) {
        return new IllegalArgumentException("Cannot encode as " + this.name() + ": " + b);
    }

    byte decPunctuation(int code) {
        return PUNCTUATION[code];
    }

    public int encTranslate(byte b) {
        if (b < 0) {
            return (0xFF & b) - 0xC0; // European chars
        }
        if (b < 0x20) {
            return encPunctuation(b); // Punctuation
        }
        if (b >= '0' && b <= '4') {
            return 0x5B + b - '0'; // Numbers
        }
        if (b >= '5' && b <= '9') {
            return 0x7B + b - '5'; // Numbers
        }
        return b; // Alphabetical
    }

    abstract int encPunctuation(byte b);

    public abstract byte decTranslate(byte codePoint);

    public static int prepareEncode(String string, byte[] data, int stringLength, boolean additionalCodecs) {
        if (stringLength == 0) {
            return 0;
        }
        int encodings = ALL_BIT_MASK;
        // filter out larger encodings in one go
        for (int i = 0; i < stringLength; i++) {
            char c = string.charAt(i);
            // non ASCII chars not supported
            if (c >= TRANSLATION_COUNT) {
                return 0;
            }
            data[i] = TRANSLATION[c];
            // remove not matching encoders
            encodings &= REMOVE_MASK[c];
            if (encodings == 0) {
                return 0;
            }
        }

        // This is hard-coded for the out-of-order UPPERHEX/LOWERHEX codecs and makes them prioritized over
        // other (larger bits-per-character) codecs that can also encode them
        if (additionalCodecs && (encodings & HEX_CODECS_COMPATIBILITY_MASK) != 0) {
            encodings &= HEX_CODECS_FILTER;
        }

        return encodings;
    }

    public static ShortStringCodec codecById(int id) {
        if (id < 0 || CODEC_BY_ID.length <= id) {
            return null;
        }
        return CODEC_BY_ID[id];
    }
}
