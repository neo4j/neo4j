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
import org.neo4j.util.VisibleForTesting;

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
                default -> CANNOT_ENCODE;
            };
        }

        @Override
        int encPunctuation(byte b) {
            return CANNOT_ENCODE;
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
                default -> CANNOT_ENCODE;
            };
        }

        @Override
        int encPunctuation(byte b) {
            return CANNOT_ENCODE;
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
            return shifted(super.encTranslate(b), 0x40);
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
            return shifted(super.encTranslate(b), 0x60);
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
            return shifted(super.encTranslate(b), 0x60);
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
                default -> CANNOT_ENCODE;
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
            return CANNOT_ENCODE;
        }

        @Override
        int encPunctuation(byte b) {
            // Handled by encTranslate
            return CANNOT_ENCODE;
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
            return shifted(EUROPEAN.encTranslate(b), 0x40);
        }

        @Override
        int encPunctuation(byte b) {
            return switch (b) {
                case 0 -> 0x00; // SPACE
                case 1 -> 0x20; // UNDERSCORE
                default -> CANNOT_ENCODE;
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
                default -> CANNOT_ENCODE;
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
                default -> CANNOT_ENCODE;
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
            return CANNOT_ENCODE;
        }

        @Override
        int encPunctuation(byte b) {
            return CANNOT_ENCODE;
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
            return CANNOT_ENCODE;
        }

        @Override
        int encPunctuation(byte b) {
            return CANNOT_ENCODE;
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

    /** What {@link #encTranslate} would answer, precomputed for every input, per codec, indexed by codec id. */
    private static final int[][] ENC_TABLE = new int[CODEC_COUNT + 2][];

    /** What {@link #encTranslate} and {@link #ENC_TABLE} answer for an input the codec cannot encode. */
    public static final int CANNOT_ENCODE = Integer.MIN_VALUE;

    /**
     * Per character, the byte it translates to in the low byte and the codecs that can encode it above, so
     * that preparing a value reads one array instead of two.
     * <p>
     * The mask is stored already narrowed to {@link #ALL_BIT_MASK}, which changes nothing: a caller's running
     * mask starts at {@code ALL_BIT_MASK} and only loses bits, so the bits dropped here could never have
     * survived the {@code and} anyway.
     */
    private static final int[] TRANSLATION_AND_MASK = new int[TRANSLATION_COUNT];

    private static final int MASK_SHIFT = Byte.SIZE;

    private static final int PREPARED_CHARS_SHIFT = Short.SIZE;

    @VisibleForTesting
    static int packedFor(int character) {
        return TRANSLATION_AND_MASK[character];
    }

    /** One {@link #TRANSLATION_AND_MASK} entry, from the codecs that <em>cannot</em> encode the character. */
    private static int pack(int removeMask, int translation) {
        return ((removeMask & ALL_BIT_MASK) << MASK_SHIFT) | (translation & 0xFF);
    }

    private static void setUp(char pos, int value, ShortStringCodec... removeCodecs) {
        TRANSLATION_AND_MASK[pos] = pack(~bitMask(removeCodecs), value);
    }

    static {
        if (Integer.numberOfLeadingZeros(ALL_BIT_MASK) < MASK_SHIFT) {
            throw new AssertionError("Codec mask no longer fits above the translation byte: " + ALL_BIT_MASK);
        }
        if (Integer.numberOfLeadingZeros(ALL_BIT_MASK) < Integer.SIZE - PREPARED_CHARS_SHIFT) {
            throw new AssertionError("Codec mask no longer fits below the character count: " + ALL_BIT_MASK);
        }
        for (ShortStringCodec codec : CODECS) {
            CODEC_BY_ID[codec.codecId] = codec;
        }
        // Every character starts unencodable: translation 0xFF and, since every codec is removed, no codec.
        Arrays.fill(TRANSLATION_AND_MASK, pack(~bitMask(CODECS), 0xFF));
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

        for (ShortStringCodec codec : CODECS) {
            int[] table = new int[TRANSLATION_COUNT];
            for (int value = 0; value < TRANSLATION_COUNT; value++) {
                table[value] = codec.encTranslate((byte) value);
            }
            ENC_TABLE[codec.codecId] = table;
        }
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

    /**
     * @return {@link #encTranslate} as a table.
     */
    public int[] encTable() {
        return ENC_TABLE[codecId];
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

    /**
     * Moves a translation from the codec it was borrowed from into this codec's range, leaving
     * {@link #CANNOT_ENCODE} alone - shifting the sentinel would overflow into a valid-looking translation.
     */
    private static int shifted(int translation, int offset) {
        return translation == CANNOT_ENCODE ? CANNOT_ENCODE : translation - offset;
    }

    byte decPunctuation(int code) {
        return PUNCTUATION[code];
    }

    /**
     * @return the character as this codec encodes it, or {@link #CANNOT_ENCODE} if it cannot. Callers that
     * picked the codec from {@link #prepareEncode} never see the sentinel, and read {@link #encTable} rather
     * than call this per character.
     */
    int encTranslate(byte b) {
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
            int packed = TRANSLATION_AND_MASK[c];
            data[i] = (byte) packed;
            // remove not matching encoders
            encodings &= packed >>> MASK_SHIFT;
            if (encodings == 0) {
                return 0;
            }
        }

        return preferHexCodecs(encodings, additionalCodecs);
    }

    /**
     * This is hard-coded for the out-of-order UPPERHEX/LOWERHEX codecs and makes them prioritized over
     * other (larger bits-per-character) codecs that can also encode them.
     */
    private static int preferHexCodecs(int encodings, boolean additionalCodecs) {
        if (additionalCodecs && (encodings & HEX_CODECS_COMPATIBILITY_MASK) != 0) {
            return encodings & HEX_CODECS_FILTER;
        }
        return encodings;
    }

    /**
     * As {@link #prepareEncode(String, byte[], int, boolean)}, reading UTF-8 bytes.
     * <p>
     * For Latin-1: a codec refuses any character at or above 256, so an encodable value is entirely
     * Latin-1, whose UTF-8 form is one byte below U+0080 and a {@code 0xC2} or {@code 0xC3}
     * pair above it. Every other byte pattern decodes to something a codec would refuse anyway - three-byte
     * sequences start at U+0800, four-byte ones become a surrogate pair, and malformed bytes become U+FFFD -
     * so "cannot read these bytes" and "no codec can encode this" are the same answer.
     *
     * @param utf8 the value's bytes.
     * @param offset where the value starts in {@code utf8}.
     * @param length its length in bytes.
     * @param data filled with the translated characters, as the String form fills it. At least
     * {@code maxChars} long.
     * @param maxChars the most characters a codec can hold; longer values answer {@link #CANNOT_ENCODE}
     * without reading any further.
     *
     * @return {@link #CANNOT_ENCODE}, or the character count and the compatible codec mask packed together.
     * Read them with {@link #charsOf} and {@link #codecsOf}.
     */
    public static int prepareEncodeUtf8(
            byte[] utf8, int offset, int length, byte[] data, int maxChars, boolean additionalCodecs) {
        int encodings = ALL_BIT_MASK;
        int chars = 0;
        int i = 0;
        while (i < length) {
            byte b = utf8[offset + i];
            int c;
            if (b >= 0) {
                c = b;
                i++;
            } else if (b == (byte) 0xC2 || b == (byte) 0xC3) {
                // The only two lead bytes Latin-1 uses. Anything else leads a sequence whose character is at
                // or above 256, which no codec takes.
                if (i + 1 == length) {
                    // Malformed, can't end with a lead. The String form would see U+FFFD here.
                    return CANNOT_ENCODE;
                }
                byte continuation = utf8[offset + i + 1];
                if ((continuation & 0xC0) != 0x80) {
                    return CANNOT_ENCODE; // Malformed, likewise.
                }
                c = ((b & 0x1F) << 6) | (continuation & 0x3F);
                i += 2;
            } else {
                return CANNOT_ENCODE;
            }

            if (chars == maxChars) {
                return CANNOT_ENCODE;
            }
            int packed = TRANSLATION_AND_MASK[c];
            data[chars++] = (byte) packed;
            encodings &= packed >>> MASK_SHIFT;
            if (encodings == 0) {
                return CANNOT_ENCODE;
            }
        }
        if (chars == 0) {
            // The String form answers 0 for an empty value, meaning no codec, and an empty value is written
            // as bytes like any other unencodable one.
            return CANNOT_ENCODE;
        }

        return (chars << PREPARED_CHARS_SHIFT) | preferHexCodecs(encodings, additionalCodecs);
    }

    /** The character count from a {@link #prepareEncodeUtf8} answer. */
    public static int charsOf(int prepared) {
        return prepared >>> PREPARED_CHARS_SHIFT;
    }

    /** The compatible codec mask from a {@link #prepareEncodeUtf8} answer. */
    public static int codecsOf(int prepared) {
        return prepared & ALL_BIT_MASK;
    }

    public static ShortStringCodec codecById(int id) {
        if (id < 0 || CODEC_BY_ID.length <= id) {
            return null;
        }
        return CODEC_BY_ID[id];
    }
}
