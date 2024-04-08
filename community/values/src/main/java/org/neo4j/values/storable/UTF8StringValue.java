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
package org.neo4j.values.storable;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOf;
import static org.neo4j.values.storable.Values.utf8Value;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.hashing.HashFunction;

/*
 * Just as a normal StringValue but is backed by a byte array and does string
 * serialization lazily when necessary.
 *
 */
public final class UTF8StringValue extends StringValue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(UTF8StringValue.class);

    /** Used for removing the high order bit from byte. */
    private static final int HIGH_BIT_MASK = 0b0111_1111;
    /** Used for detecting non-continuation bytes. For example {@code 0b10xx_xxxx}. */
    private static final int NON_CONTINUATION_BIT_MASK = 0b0100_0000;

    private volatile String value;
    private final byte[] bytes;
    private final int offset;
    private final int byteLength;

    UTF8StringValue(byte[] bytes, int offset, int length) {
        assert bytes != null;
        this.bytes = bytes;
        this.offset = offset;
        this.byteLength = length;
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeUTF8(bytes, offset, byteLength);
    }

    @Override
    public boolean equals(Value value) {
        if (value instanceof UTF8StringValue other) {
            return Arrays.equals(
                    bytes, offset, offset + byteLength, other.bytes, other.offset, other.offset + other.byteLength);
        } else {
            return super.equals(value);
        }
    }

    @Override
    protected String value() {
        String s = value;
        if (s == null) {
            synchronized (this) {
                s = value;
                if (s == null) {
                    value = s = new String(bytes, offset, byteLength, StandardCharsets.UTF_8);
                }
            }
        }
        return s;
    }

    @Override
    public int length() {
        return numberOfCodePoints(bytes, offset, byteLength);
    }

    @Override
    public boolean isEmpty() {
        return bytes.length == 0 || byteLength == 0;
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOf(bytes);
    }

    private static int numberOfCodePoints(byte[] bytes, int offset, int byteLength) {
        int count = 0, i = offset, len = offset + byteLength;
        while (i < len) {
            byte b = bytes[i];
            // If high bit is zero (equivalent to the byte being positive in two's complement)
            // we are dealing with an ascii value and use a single byte for storing the value.
            if (b >= 0) {
                i++;
                count++;
                continue;
            }

            // The number of high bits tells us how many bytes we use to store the value
            // e.g. 110xxxx -> need two bytes, 1110xxxx -> need three bytes, 11110xxx -> needs
            // four bytes
            while (b < 0) {
                i++;
                b = (byte) (b << 1);
            }
            count++;
        }
        return count;
    }

    @Override
    protected int computeHashToMemoize() {
        if (bytes.length == 0 || byteLength == 0) {
            return 0;
        }

        CodePointCursor cpc = new CodePointCursor(bytes, offset);
        int hash = 1;
        int len = offset + byteLength;

        while (cpc.i < len) {
            hash = HASH_CONSTANT * hash + (int) cpc.nextCodePoint();
        }
        return hash;
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        CodePointCursor cpc = new CodePointCursor(bytes, offset);
        int len = offset + byteLength;

        while (cpc.i < len) {
            long codePointA = cpc.nextCodePoint() << 32;
            long codePointB = 0L;
            if (cpc.i < len) {
                codePointB = cpc.nextCodePoint();
            }
            hash = hashFunction.update(hash, codePointA + codePointB);
        }

        return hashFunction.update(hash, cpc.codePointCount);
    }

    public static final class CodePointCursor {
        private final byte[] values;
        private int i;
        private int codePointCount;

        public CodePointCursor(byte[] values, int offset) {
            this.values = values;
            this.i = offset;
        }

        public long nextCodePoint() {
            codePointCount++;
            byte b = values[i];
            // If high bit is zero (equivalent to the byte being positive in two's complement)
            // we are dealing with an ascii value and use a single byte for storing the value.
            if (b >= 0) {
                i++;
                return b;
            }

            // We can now have one of three situations.
            // Byte1    Byte2    Byte3    Byte4
            // 110xxxxx 10xxxxxx
            // 1110xxxx 10xxxxxx 10xxxxxx
            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            // Figure out how many bytes we need by reading the number of leading bytes
            int bytesNeeded = 0;
            while (b < 0) {
                bytesNeeded++;
                b = (byte) (b << 1);
            }
            int codePoint = codePoint(values, b, i, bytesNeeded);
            i += bytesNeeded;
            return codePoint;
        }
    }

    public static final class ReverseCodePointCursor {
        private final byte[] values;
        private int i;

        public ReverseCodePointCursor(byte[] values, int offset, int byteLength) {
            this.values = values;
            this.i = offset + byteLength - 1;
        }

        public long previousCodePoint() {
            byte b = values[i];
            // If high bit is zero (equivalent to the byte being positive in two's complement)
            // we are dealing with an ascii value and use a single byte for storing the value.
            if (b >= 0) {
                i--;
                return b;
            }

            // We can now have one of three situations.
            // Byte1    Byte2    Byte3    Byte4
            // 110xxxxx 10xxxxxx
            // 1110xxxx 10xxxxxx 10xxxxxx
            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            // Figure out how many bytes we need by reading the number of leading bytes
            int bytesNeeded = 1;
            while ((b & NON_CONTINUATION_BIT_MASK) == 0) {
                bytesNeeded++;
                b = values[--i];
            }
            int codePoint = codePoint(values, (byte) (b << bytesNeeded), i, bytesNeeded);
            i -= 1;
            return codePoint;
        }
    }

    @Override
    public TextValue substring(int start, int length) {
        if (start < 0 || length < 0) {
            throw new IndexOutOfBoundsException("Cannot handle negative start index nor negative length");
        }
        if (length == 0) {
            return StringValue.EMPTY;
        }

        int end = start + length;
        byte[] values = bytes;
        int count = 0, byteStart = -1, byteEnd = -1, i = offset, len = offset + byteLength;
        while (i < len) {
            if (count == start) {
                byteStart = i;
            }
            if (count == end) {
                byteEnd = i;
                break;
            }
            byte b = values[i];
            // If high bit is zero (equivalent to the byte being positive in two's complement)
            // we are dealing with an ascii value and use a single byte for storing the value.
            if (b >= 0) {
                i++;
            }

            while (b < 0) {
                i++;
                b = (byte) (b << 1);
            }
            count++;
        }
        if (byteEnd < 0) {
            byteEnd = len;
        }
        if (byteStart < 0) {
            return StringValue.EMPTY;
        }
        return new UTF8StringValue(values, byteStart, byteEnd - byteStart);
    }

    @Override
    public TextValue trim() {
        byte[] values = bytes;

        if (values.length == 0 || byteLength == 0) {
            return this;
        }

        int startIndex = trimLeftIndexWhitespace();
        int endIndex = trimRightIndexWhitespace();
        if (startIndex > endIndex) {
            return StringValue.EMPTY;
        }

        return new UTF8StringValue(values, startIndex, Math.max(endIndex + 1 - startIndex, 0));
    }

    @Override
    public TextValue ltrim() {
        byte[] values = bytes;
        if (values.length == 0 || byteLength == 0) {
            return this;
        }

        int startIndex = trimLeftIndexWhitespace();
        assert (startIndex <= values.length);
        if (startIndex == offset) {
            return this;
        } else {
            return new UTF8StringValue(values, startIndex, byteLength - (startIndex - offset));
        }
    }

    @Override
    public TextValue rtrim() {
        byte[] values = bytes;
        if (values.length == 0 || byteLength == 0) {
            return this;
        }

        int endIndex = trimRightIndexWhitespace();
        if (endIndex < 0) {
            return StringValue.EMPTY;
        }
        return new UTF8StringValue(values, offset, endIndex + 1 - offset);
    }

    @Override
    public TextValue trim(TextValue trimCharacterString) {
        byte[] values = bytes;

        if (values.length == 0 || byteLength == 0) {
            return this;
        }
        int startIndex = trimLeftIndex(trimCharacterString);
        int endIndex = trimRightIndex(trimCharacterString);
        if (startIndex > endIndex) {
            return StringValue.EMPTY;
        }

        return new UTF8StringValue(values, startIndex, Math.max(endIndex + 1 - startIndex, 0));
    }

    @Override
    public TextValue ltrim(TextValue trimCharacterString) {
        byte[] values = bytes;
        if (values.length == 0 || byteLength == 0) {
            return this;
        }

        int startIndex = trimLeftIndex(trimCharacterString);
        assert (startIndex <= values.length);
        if (startIndex == offset) {
            return this;
        } else {
            return new UTF8StringValue(values, startIndex, byteLength - (startIndex - offset));
        }
    }

    @Override
    public TextValue rtrim(TextValue trimCharacterString) {
        byte[] values = bytes;
        if (values.length == 0 || byteLength == 0) {
            return this;
        }

        int endIndex = trimRightIndex(trimCharacterString);
        if (endIndex < 0) {
            return StringValue.EMPTY;
        }
        return new UTF8StringValue(values, offset, endIndex + 1 - offset);
    }

    @Override
    public TextValue plus(TextValue other) {
        if (other instanceof UTF8StringValue rhs) {
            byte[] newBytes = new byte[byteLength + rhs.byteLength];
            System.arraycopy(bytes, offset, newBytes, 0, byteLength);
            System.arraycopy(rhs.bytes, rhs.offset, newBytes, byteLength, rhs.byteLength);
            return utf8Value(newBytes);
        }

        return Values.stringValue(stringValue() + other.stringValue());
    }

    @Override
    public boolean startsWith(TextValue other) {

        if (other instanceof UTF8StringValue suffix) {
            return startsWith(suffix, 0);
        }

        return value().startsWith(other.stringValue());
    }

    @Override
    public boolean endsWith(TextValue other) {

        if (other instanceof UTF8StringValue suffix) {
            return startsWith(suffix, byteLength - suffix.byteLength);
        }

        return value().endsWith(other.stringValue());
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public boolean contains(TextValue other) {

        if (other instanceof final UTF8StringValue substring) {
            if (byteLength == 0) {
                return substring.byteLength == 0;
            }
            if (substring.byteLength == 0) {
                return true;
            }
            if (substring.byteLength > byteLength) {
                return false;
            }

            final byte first = substring.bytes[substring.offset];
            final int max = offset + byteLength - substring.byteLength;
            for (int pos = offset; pos <= max; pos++) {
                // find first byte
                if (bytes[pos] != first) {
                    while (++pos <= max && bytes[pos] != first) {
                        // do nothing
                    }
                }

                // Now we have the first byte match, look at the rest
                if (pos <= max) {
                    int i = pos + 1;
                    final int end = pos + substring.byteLength;
                    for (int j = substring.offset + 1; i < end && bytes[i] == substring.bytes[j]; j++, i++) {
                        // do nothing
                    }

                    if (i == end) {
                        return true;
                    }
                }
            }
            return false;
        }

        return value().contains(other.stringValue());
    }

    private boolean startsWith(UTF8StringValue prefix, int startPos) {
        int thisOffset = offset + startPos;
        int prefixOffset = prefix.offset;
        int prefixCount = prefix.byteLength;
        if (startPos < 0 || prefixCount > byteLength) {
            return false;
        }

        while (--prefixCount >= 0) {
            if (bytes[thisOffset++] != prefix.bytes[prefixOffset++]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public TextValue reverse() {
        byte[] values = bytes;

        if (values.length == 0 || byteLength == 0) {
            return StringValue.EMPTY;
        }

        int i = offset, len = offset + byteLength;
        byte[] newValues = new byte[byteLength];
        while (i < len) {
            byte b = values[i];
            // If high bit is zero (equivalent to the byte being positive in two's complement)
            // we are dealing with an ascii value and use a single byte for storing the value.
            if (b >= 0) {
                // a single byte is trivial to reverse
                // just put it at the opposite end of the new array
                newValues[len - 1 - i] = b;
                i++;
                continue;
            }

            // We can now have one of three situations.
            // Byte1    Byte2    Byte3    Byte4
            // 110xxxxx 10xxxxxx
            // 1110xxxx 10xxxxxx 10xxxxxx
            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            // Figure out how many bytes we need by reading the number of leading bytes
            int bytesNeeded = 0;
            while (b < 0) {
                bytesNeeded++;
                b = (byte) (b << 1);
            }
            // reversing when multiple bytes are needed for the code point we cannot just reverse
            // since we need to preserve the code point while moving it,
            // e.g. [A, b1,b2, B] -> [B, b1,b2, A]
            System.arraycopy(values, i, newValues, len - i - bytesNeeded, bytesNeeded);
            i += bytesNeeded;
        }

        return new UTF8StringValue(newValues, 0, newValues.length);
    }

    @Override
    public int compareTo(TextValue other) {
        if (!(other instanceof UTF8StringValue otherUTF8)) {
            return super.compareTo(other);
        }
        return byteArrayCompare(bytes, offset, byteLength, otherUTF8.bytes, otherUTF8.offset, otherUTF8.byteLength);
    }

    private static int byteArrayCompare(
            byte[] value1, int value1Offset, int value1Length, byte[] value2, int value2Offset, int value2Length) {
        int lim = Math.min(value1Length, value2Length);
        for (int i = 0; i < lim; i++) {
            byte b1 = value1[i + value1Offset];
            byte b2 = value2[i + value2Offset];
            if (b1 != b2) {
                return (((int) b1) & 0xFF) - (((int) b2) & 0xFF);
            }
        }
        return value1Length - value2Length;
    }

    @Override
    protected Matcher matcher(Pattern pattern) {
        return pattern.matcher(value());
    }

    /**
     * Returns the left-most index into the underlying byte array that does not belong to a whitespace code point
     */
    private int trimLeftIndexWhitespace() {
        int i = offset, len = offset + byteLength;
        while (i < len) {
            byte b = bytes[i];
            // If high bit is zero (equivalent to the byte being positive in two's complement)
            // we are dealing with an ascii value and use a single byte for storing the value.
            if (b >= 0) {
                if (!Character.isWhitespace(b)) {
                    return i;
                }
                i++;
                continue;
            }

            // We can now have one of three situations.
            // Byte1    Byte2    Byte3    Byte4
            // 110xxxxx 10xxxxxx
            // 1110xxxx 10xxxxxx 10xxxxxx
            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            // Figure out how many bytes we need by reading the number of leading bytes
            int bytesNeeded = 0;
            while (b < 0) {
                bytesNeeded++;
                b = (byte) (b << 1);
            }
            int codePoint = codePoint(bytes, b, i, bytesNeeded);
            if (!Character.isWhitespace(codePoint)) {
                return i;
            }
            i += bytesNeeded;
        }
        return i;
    }

    /**
     * Returns the left-most index into the underlying byte array that does not exist in the given trimCharList
     */
    private int trimLeftIndex(TextValue trimCharacterString) {
        int pos = offset;
        int[] trimCharacterStringCodePointArray =
                trimCharacterString.stringValue().codePoints().toArray();
        if (trimCharacterString.isEmpty()) return pos;
        var cpc = new CodePointCursor(bytes, offset);
        while (cpc.i < byteLength + offset) {
            pos = cpc.i;
            var cp = cpc.nextCodePoint();
            if (!ArrayUtils.contains(trimCharacterStringCodePointArray, (int) cp)) {
                return pos;
            }
            pos = cpc.i;
        }
        return pos;
    }

    /**
     * Returns the right-most index into the underlying byte array that does not belong to a whitespace code point
     */
    private int trimRightIndexWhitespace() {
        int index = offset + byteLength - 1;
        while (index >= offset) {
            byte b = bytes[index];
            // If high bit is zero (equivalent to the byte being positive in two's complement)
            // we are dealing with an ascii value and use a single byte for storing the value.
            if (b >= 0) {
                if (!Character.isWhitespace(b)) {
                    return index;
                }
                index--;
                continue;
            }

            // We can now have one of three situations.
            // Byte1    Byte2    Byte3    Byte4
            // 110xxxxx 10xxxxxx
            // 1110xxxx 10xxxxxx 10xxxxxx
            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            int bytesNeeded = 1;
            while ((b & NON_CONTINUATION_BIT_MASK) == 0) {
                bytesNeeded++;
                b = bytes[--index];
            }

            int codePoint = codePoint(bytes, (byte) (b << bytesNeeded), index, bytesNeeded);
            if (!Character.isWhitespace(codePoint)) {
                return Math.min(index + bytesNeeded - 1, bytes.length - 1);
            }
            index--;
        }
        return index;
    }

    /**
     * Returns the right-most index into the underlying byte array that does not exist in the given trimCharList
     */
    private int trimRightIndex(TextValue trimCharacterString) {
        int pos = offset + byteLength - 1;
        int[] trimCharacterStringCodePointArray =
                trimCharacterString.stringValue().codePoints().toArray();
        if (trimCharacterString.isEmpty()) return pos;
        var cpc = new ReverseCodePointCursor(bytes, offset, byteLength);
        while (cpc.i > 0) {
            pos = cpc.i;
            var cp = cpc.previousCodePoint();
            if (!ArrayUtils.contains(trimCharacterStringCodePointArray, (int) cp)) {
                return pos;
            }
            pos = cpc.i;
        }
        return pos;
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.UTF8_TEXT;
    }

    public byte[] bytes() {
        return bytes;
    }

    private static int codePoint(byte[] bytes, byte currentByte, int i, int bytesNeeded) {
        return switch (bytesNeeded) {
            case 2 -> (currentByte << 4) | (bytes[i + 1] & HIGH_BIT_MASK);
            case 3 -> (currentByte << 9) | ((bytes[i + 1] & HIGH_BIT_MASK) << 6) | (bytes[i + 2] & HIGH_BIT_MASK);
            case 4 -> (currentByte << 14)
                    | ((bytes[i + 1] & HIGH_BIT_MASK) << 12)
                    | ((bytes[i + 2] & HIGH_BIT_MASK) << 6)
                    | (bytes[i + 3] & HIGH_BIT_MASK);
            default -> throw new IllegalArgumentException("Malformed UTF8 value");
        };
    }
}
