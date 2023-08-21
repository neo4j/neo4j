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
package org.neo4j.kernel.impl.store;

import static org.neo4j.internal.codec.ShortStringCodec.ALPHANUM;
import static org.neo4j.internal.codec.ShortStringCodec.ALPHASYM;
import static org.neo4j.internal.codec.ShortStringCodec.CODECS;
import static org.neo4j.internal.codec.ShortStringCodec.ENCODING_LATIN1;
import static org.neo4j.internal.codec.ShortStringCodec.ENCODING_UTF8;
import static org.neo4j.internal.codec.ShortStringCodec.EUROPEAN;
import static org.neo4j.internal.codec.ShortStringCodec.URI;
import static org.neo4j.internal.codec.ShortStringCodec.bitMask;
import static org.neo4j.internal.codec.ShortStringCodec.codecById;
import static org.neo4j.internal.codec.ShortStringCodec.prepareEncode;

import java.nio.charset.StandardCharsets;
import org.neo4j.internal.codec.ShortStringCodec;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.util.BitBuffer;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

/**
 * Supports encoding alphanumerical and <code>SP . - + , ' : / _</code>
 *
 * (This version assumes 14bytes property block, instead of 8bytes)
 */
public class LongerShortString {
    private static final int HEADER_SIZE = 39; // bits
    private static final int REMOVE_LARGE_ENCODINGS_MASK = ~bitMask(ALPHANUM, ALPHASYM, URI, EUROPEAN);

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
    public static boolean encode(int keyId, String string, PropertyBlock target, int payloadSize) {
        // NUMERICAL can carry most characters, so compare to that
        int dataLength = string.length();
        // We only use 6 bits for storing the string length
        // TODO could be dealt with by having string length zero and go for null bytes,
        // at least for LATIN1 (that's what the ShortString implementation initially did)
        if (dataLength > maxLength(ShortStringCodec.NUMERICAL, payloadSize) || dataLength > 63) {
            return false; // Not handled by any encoding
        }

        // Allocate space for the intermediate representation
        // (using the intermediate representation table above)
        byte[] data = new byte[dataLength];

        // Keep track of the possible encodings that can be used for the string
        // 0 means none applies
        int codecs = prepareEncode(string, data, dataLength, false);
        if (dataLength > maxLength(ALPHANUM, payloadSize)) {
            codecs &= REMOVE_LARGE_ENCODINGS_MASK;
        }

        if (codecs != 0 && tryEncode(codecs, keyId, target, payloadSize, data, dataLength)) {
            return true;
        }
        return encodeWithCharSet(keyId, string, target, payloadSize, dataLength);
    }

    private static boolean encodeWithCharSet(
            int keyId, String string, PropertyBlock target, int payloadSize, int stringLength) {
        int maxBytes = PropertyType.getPayloadSize();
        if (stringLength <= maxBytes - 5) {
            return encodeLatin1(keyId, string, target) || encodeUTF8(keyId, string, target, payloadSize);
        }
        return false;
    }

    private static boolean tryEncode(
            int encodings, int keyId, PropertyBlock target, int payloadSize, byte[] data, final int length) {
        // find encoders in order that are still selected and try to encode the data
        for (ShortStringCodec codec : CODECS) {
            if ((codec.bitMask() & encodings) == 0) {
                continue;
            }
            if (doEncode(keyId, data, target, payloadSize, length, codec)) {
                return true;
            }
        }
        return false;
    }

    private static void writeHeader(BitBuffer bits, int keyId, int encoding, int stringLength) {
        // [][][][ lll,llle][eeee,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        bits.put(keyId, 24)
                .put(PropertyType.SHORT_STRING.intValue(), 4)
                .put(encoding, 5)
                .put(stringLength, 6);
    }

    /**
     * Decode a short string represented as a long[]
     *
     * @param block the value to decode to a short string.
     * @return the decoded short string
     */
    public static TextValue decode(PropertyBlock block) {
        return decode(block.getValueBlocks(), 0);
    }

    public static TextValue decode(long[] blocks, int offset) {
        long firstLong = blocks[offset];
        if ((firstLong & 0xFFFFFF0FFFFFFFFFL) == 0) {
            return Values.EMPTY_STRING;
        }
        // key(24b) + type(4) = 28
        int codecId = (int) ((firstLong & 0x1F0000000L) >>> 28); // 5 bits of encoding
        int stringLength = (int) ((firstLong & 0x7E00000000L) >>> 33); // 6 bits of stringLength
        if (codecId == ENCODING_UTF8) {
            return decodeUTF8(blocks, offset, stringLength);
        }
        if (codecId == ENCODING_LATIN1) {
            return decodeLatin1(blocks, offset, stringLength);
        }

        ShortStringCodec table = codecById(codecId);
        assert table != null
                : "We only decode LongerShortStrings after we have consistently read the PropertyBlock "
                        + "data from the page cache. Thus, we should never have an invalid encoding header here.";
        if (table.needsChars()) {
            // Special since it's an encoding containing characters, which although within one byte, is larger than 127
            // and so
            // would becomes negative and that doesn't sit well with utf-8. So in this case we need to go the char[]
            // route
            char[] result = new char[stringLength];
            decode(result, blocks, offset, table);
            // We know the char array is unshared, so use sharing constructor explicitly
            return Values.stringValue(new String(result));
        }

        // All resulting characters is within byte value 0-127 and so fits in one non-negative byte,
        // therefore we can deserialize this straight into a byte[], resulting in a more efficient UTF8StringValue
        // This code path is for the majority of the encodings.
        byte[] result = new byte[stringLength];
        decode(result, blocks, offset, table);
        return Values.utf8Value(result);
    }

    private static void decode(byte[] result, long[] blocks, int offset, ShortStringCodec table) {
        // encode shifts in the bytes with the first at the MSB, therefore
        // we must "unshift" in the reverse order
        int block = offset;
        int maskShift = HEADER_SIZE;
        long baseMask = table.mask();
        for (int i = 0; i < result.length; i++) {
            byte codePoint = (byte) ((blocks[block] >>> maskShift) & baseMask);
            maskShift += table.bitsPerCharacter();
            if (maskShift >= 64 && block + 1 < blocks.length) {
                maskShift %= 64;
                codePoint |= (blocks[++block] & (baseMask >>> (table.bitsPerCharacter() - maskShift)))
                        << (table.bitsPerCharacter() - maskShift);
            }
            result[i] = table.decTranslate(codePoint);
        }
    }

    private static void decode(char[] result, long[] blocks, int offset, ShortStringCodec table) {
        // encode shifts in the bytes with the first at the MSB, therefore
        // we must "unshift" in the reverse order
        int block = offset;
        int maskShift = HEADER_SIZE;
        long baseMask = table.mask();
        for (int i = 0; i < result.length; i++) {
            byte codePoint = (byte) ((blocks[block] >>> maskShift) & baseMask);
            maskShift += table.bitsPerCharacter();
            if (maskShift >= 64 && block + 1 < blocks.length) {
                maskShift %= 64;
                codePoint |= (blocks[++block] & (baseMask >>> (table.bitsPerCharacter() - maskShift)))
                        << (table.bitsPerCharacter() - maskShift);
            }
            result[i] = (char) (table.decTranslate(codePoint) & 0xFF);
        }
    }

    private static BitBuffer newBits(ShortStringCodec codec, int length) {
        return BitBuffer.bits(calculateNumberOfBlocksUsed(codec, length) * 8);
    }

    private static BitBuffer newBitsForStep8(int length) {
        return BitBuffer.bits(calculateNumberOfBlocksUsedForStep8(length) << 3); // *8
    }

    private static boolean encodeLatin1(int keyId, String string, PropertyBlock target) {
        int length = string.length();
        BitBuffer bits = newBitsForStep8(length);
        writeHeader(bits, keyId, ENCODING_LATIN1, length);
        if (!writeLatin1Characters(string, bits)) {
            return false;
        }
        target.setValueBlocks(bits.getLongs());
        return true;
    }

    public static boolean writeLatin1Characters(String string, BitBuffer bits) {
        int length = string.length();
        for (int i = 0; i < length; i++) {
            char c = string.charAt(i);
            if (c >= 256) {
                return false;
            }
            bits.put(c, 8); // Just the lower byte
        }
        return true;
    }

    private static boolean encodeUTF8(int keyId, String string, PropertyBlock target, int payloadSize) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        final int length = bytes.length;
        if (length > payloadSize - 3 /*key*/ - 2 /*enc+len*/) {
            return false;
        }
        BitBuffer bits = newBitsForStep8(length);
        writeHeader(
                bits, keyId, ENCODING_UTF8, length); // In this case it isn't the string length, but the number of bytes
        for (byte value : bytes) {
            bits.put(value);
        }
        target.setValueBlocks(bits.getLongs());
        return true;
    }

    private static int maxLength(ShortStringCodec codec, int payloadSize) {
        // key-type-encoding-length
        return ((payloadSize << 3) - 24 - 4 - 4 - 6) / codec.bitsPerCharacter();
    }

    private static boolean doEncode(
            int keyId, byte[] data, PropertyBlock target, int payloadSize, final int length, ShortStringCodec codec) {
        if (length > maxLength(codec, payloadSize)) {
            return false;
        }
        BitBuffer bits = newBits(codec, length);
        writeHeader(bits, keyId, codec.id(), length);
        if (length > 0) {
            translateData(bits, data, length, codec.bitsPerCharacter(), codec);
        }
        target.setValueBlocks(bits.getLongs());
        return true;
    }

    private static void translateData(BitBuffer bits, byte[] data, int length, final int step, ShortStringCodec codec) {
        for (int i = 0; i < length; i++) {
            bits.put(codec.encTranslate(data[i]), step);
        }
    }

    private static TextValue decodeLatin1(long[] blocks, int offset, int stringLength) {
        char[] result = new char[stringLength];
        int block = offset;
        int maskShift = HEADER_SIZE;
        for (int i = 0; i < result.length; i++) {
            char codePoint = (char) ((blocks[block] >>> maskShift) & 0xFF);
            maskShift += 8;
            if (maskShift >= 64) {
                maskShift %= 64;
                codePoint |= (blocks[++block] & (0xFF >>> (8 - maskShift))) << (8 - maskShift);
            }
            result[i] = codePoint;
        }
        return Values.stringValue(new String(result));
    }

    private static TextValue decodeUTF8(long[] blocks, int offset, int stringLength) {
        byte[] result = new byte[stringLength];
        int block = offset;
        int maskShift = HEADER_SIZE;
        for (int i = 0; i < result.length; i++) {
            byte codePoint = (byte) (blocks[block] >>> maskShift);
            maskShift += 8;
            if (maskShift >= 64) {
                maskShift %= 64;
                codePoint |= (blocks[++block] & (0xFF >>> (8 - maskShift))) << (8 - maskShift);
            }
            result[i] = codePoint;
        }
        return Values.utf8Value(result);
    }

    public static int calculateNumberOfBlocksUsed(long firstBlock) {
        /*
         * [ lll,llle][eeee,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
         */
        int codecId = (int) ((firstBlock & 0x1F0000000L) >> 28);
        int length = (int) ((firstBlock & 0x7E00000000L) >> 33);
        if (codecId == ENCODING_UTF8 || codecId == ENCODING_LATIN1) {
            return calculateNumberOfBlocksUsedForStep8(length);
        }

        ShortStringCodec codec = codecById(codecId);
        if (codec == null) {
            // We probably did an inconsistent read of the first block
            return PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING;
        }
        return calculateNumberOfBlocksUsed(codec, length);
    }

    public static int calculateNumberOfBlocksUsedForStep8(int length) {
        return totalBits(length << 3); // * 8
    }

    public static int calculateNumberOfBlocksUsed(ShortStringCodec codec, int length) {
        return totalBits(length * codec.bitsPerCharacter());
    }

    private static int totalBits(int bitsForCharacters) {
        int bitsInTotal = 24 + 4 + 5 + 6 + bitsForCharacters;
        return ((bitsInTotal - 1) >> 6) + 1; // /64
    }
}
