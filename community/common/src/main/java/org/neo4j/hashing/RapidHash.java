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
package org.neo4j.hashing;

import static java.lang.Math.unsignedMultiplyHigh;
import static org.neo4j.internal.helpers.VarHandleUtils.byteArrayViewVarHandle;

import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * RapidHash v3 ported to Java.
 * <p>
 * The 128-bit hash is two of these 64-bit hashes computed in a single pass over the input: the low half under
 * the hasher's seed and the high half under a second seed derived from it. Each half carries its own state
 * through the whole input, so the pair collides only when two independent 64-bit hashes do.
 * <p>
 * <a href="https://github.com/Nicoshev/rapidhash">rapidhash</a>
 */
public final class RapidHash implements Hasher64, Hasher128 {
    private static final VarHandle LE_INTEGER = byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LE_LONG = byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final long SECRET0 = 0x2d358dccaa6c78a5L;
    private static final long SECRET1 = 0x8bb84b93962eacc9L;
    private static final long SECRET2 = 0x4b33a62ed433d4a3L;
    private static final long SECRET3 = 0x4d5a2da51de1aa47L;
    private static final long SECRET4 = 0xa0761d6478bd642fL;
    private static final long SECRET5 = 0xe7037ed1a0b428dbL;
    private static final long SECRET6 = 0x90ed1765281c388cL;
    private static final long SECRET7 = 0xaaaaaaaaaaaaaaaaL;
    static final long HIGH_SEED = 0x9e3779b97f4a7c15L;
    private static final RapidHash DEFAULT_HASHER_INSTANCE = create(0L);

    private final long seed;
    private final long highSeed;

    private RapidHash(long seed) {
        this.seed = premix(seed);
        this.highSeed = premix(seed ^ HIGH_SEED);
    }

    private static long premix(long seed) {
        return seed ^ mix(seed ^ SECRET2, SECRET1);
    }

    /**
     * Create a new {@link RapidHash} instance with the default seed.
     *
     * @return the default hasher instance.
     */
    public static RapidHash create() {
        return DEFAULT_HASHER_INSTANCE;
    }

    /**
     * Create a new {@link RapidHash} instance with the given seed.
     *
     * @param seed the seed value for the hasher
     * @return a new hasher instance with the specified seed.
     */
    public static RapidHash create(long seed) {
        return new RapidHash(seed);
    }

    /**
     * Hashes a single long value.
     *
     * @param l the long value to hash.
     * @return the hash value.
     */
    @Override
    public long hash(long l) {
        long a = l ^ SECRET1;
        long b = l ^ (seed ^ 8);
        return mix((a * b) ^ SECRET7, unsignedMultiplyHigh(a, b) ^ (SECRET1 ^ 8));
    }

    /**
     * Hash a byte array with the standard RapidHash algorithm.
     *
     * @param key the byte array to hash.
     * @param offset the offset in the byte array to start hashing from.
     * @param length the length of the byte array to hash.
     * @return the hash value.
     */
    @Override
    public long hash(byte[] key, int offset, int length) {
        return hashInternal(key, offset, length, seed);
    }

    /**
     * {@inheritDoc}
     * <p>
     * {@code out[0]} is what {@link #hash(byte[], int, int)} returns for the same bytes, and {@code out[1]} is
     * what it returns under the seed {@code seed ^ HIGH_SEED}.
     */
    @Override
    public void hashBytesTo128(ByteBuffer key, int offset, int len, long[] out) {
        if (key.order() != ByteOrder.LITTLE_ENDIAN) {
            throw new IllegalArgumentException("This hash reads eight bytes at a time and needs a "
                    + "little-endian buffer to give the same answer as it would for the same bytes in an array");
        }
        long lo = seed, hi = highSeed;
        long a, b;
        int i = len;
        if (len <= 16) {
            if (len >= 4) {
                lo ^= len;
                hi ^= len;
                if (len >= 8) {
                    a = i64(key, offset);
                    b = i64(key, offset + len - 8);
                } else {
                    a = u32(key, offset);
                    b = u32(key, offset + len - 4);
                }
            } else if (len > 0) {
                a = ((key.get(offset) & 0xFFL) << 45) | (key.get(offset + len - 1) & 0xFFL);
                b = key.get(offset + (len >> 1)) & 0xFFL;
            } else a = b = 0;
        } else {
            if (len > 112) {
                long lo1 = lo, lo2 = lo, lo3 = lo, lo4 = lo, lo5 = lo, lo6 = lo;
                long hi1 = hi, hi2 = hi, hi3 = hi, hi4 = hi, hi5 = hi, hi6 = hi;

                do {
                    long x0 = i64(key, offset) ^ SECRET0, y0 = i64(key, offset + 8);
                    long x1 = i64(key, offset + 16) ^ SECRET1, y1 = i64(key, offset + 24);
                    long x2 = i64(key, offset + 32) ^ SECRET2, y2 = i64(key, offset + 40);
                    long x3 = i64(key, offset + 48) ^ SECRET3, y3 = i64(key, offset + 56);
                    long x4 = i64(key, offset + 64) ^ SECRET4, y4 = i64(key, offset + 72);
                    long x5 = i64(key, offset + 80) ^ SECRET5, y5 = i64(key, offset + 88);
                    long x6 = i64(key, offset + 96) ^ SECRET6, y6 = i64(key, offset + 104);
                    lo = mix(x0, y0 ^ lo);
                    lo1 = mix(x1, y1 ^ lo1);
                    lo2 = mix(x2, y2 ^ lo2);
                    lo3 = mix(x3, y3 ^ lo3);
                    lo4 = mix(x4, y4 ^ lo4);
                    lo5 = mix(x5, y5 ^ lo5);
                    lo6 = mix(x6, y6 ^ lo6);
                    hi = mix(x0, y0 ^ hi);
                    hi1 = mix(x1, y1 ^ hi1);
                    hi2 = mix(x2, y2 ^ hi2);
                    hi3 = mix(x3, y3 ^ hi3);
                    hi4 = mix(x4, y4 ^ hi4);
                    hi5 = mix(x5, y5 ^ hi5);
                    hi6 = mix(x6, y6 ^ hi6);
                    offset += 112;
                    i -= 112;
                } while (i > 112);

                lo ^= lo1 ^ lo2 ^ lo3 ^ lo4 ^ lo5 ^ lo6;
                hi ^= hi1 ^ hi2 ^ hi3 ^ hi4 ^ hi5 ^ hi6;
            }
            if (i > 16) {
                long x = i64(key, offset) ^ SECRET2, y = i64(key, offset + 8);
                lo = mix(x, y ^ lo);
                hi = mix(x, y ^ hi);
                if (i > 32) {
                    x = i64(key, offset + 16) ^ SECRET2;
                    y = i64(key, offset + 24);
                    lo = mix(x, y ^ lo);
                    hi = mix(x, y ^ hi);
                    if (i > 48) {
                        x = i64(key, offset + 32) ^ SECRET1;
                        y = i64(key, offset + 40);
                        lo = mix(x, y ^ lo);
                        hi = mix(x, y ^ hi);
                        if (i > 64) {
                            x = i64(key, offset + 48) ^ SECRET1;
                            y = i64(key, offset + 56);
                            lo = mix(x, y ^ lo);
                            hi = mix(x, y ^ hi);
                            if (i > 80) {
                                x = i64(key, offset + 64) ^ SECRET2;
                                y = i64(key, offset + 72);
                                lo = mix(x, y ^ lo);
                                hi = mix(x, y ^ hi);
                                if (i > 96) {
                                    x = i64(key, offset + 80) ^ SECRET1;
                                    y = i64(key, offset + 88);
                                    lo = mix(x, y ^ lo);
                                    hi = mix(x, y ^ hi);
                                }
                            }
                        }
                    }
                }
            }
            a = i64(key, offset + i - 16) ^ i;
            b = i64(key, offset + i - 8);
        }
        out[0] = finish(a, b, lo, i);
        out[1] = finish(a, b, hi, i);
    }

    private static long hashInternal(byte[] key, int offset, int len, long seed) {
        long a, b;
        int i = len;
        if (len <= 16) {
            if (len >= 4) {
                seed ^= len;
                if (len >= 8) {
                    a = i64(key, offset);
                    b = i64(key, offset + len - 8);
                } else {
                    a = u32(key, offset);
                    b = u32(key, offset + len - 4);
                }
            } else if (len > 0) {
                a = ((key[offset] & 0xFFL) << 45) | (key[offset + len - 1] & 0xFFL);
                b = key[offset + (len >> 1)] & 0xFFL;
            } else a = b = 0;
        } else {
            if (len > 112) {
                long see1 = seed, see2 = seed;
                long see3 = seed, see4 = seed;
                long see5 = seed, see6 = seed;

                do {
                    seed = mix(i64(key, offset) ^ SECRET0, i64(key, offset + 8) ^ seed);
                    see1 = mix(i64(key, offset + 16) ^ SECRET1, i64(key, offset + 24) ^ see1);
                    see2 = mix(i64(key, offset + 32) ^ SECRET2, i64(key, offset + 40) ^ see2);
                    see3 = mix(i64(key, offset + 48) ^ SECRET3, i64(key, offset + 56) ^ see3);
                    see4 = mix(i64(key, offset + 64) ^ SECRET4, i64(key, offset + 72) ^ see4);
                    see5 = mix(i64(key, offset + 80) ^ SECRET5, i64(key, offset + 88) ^ see5);
                    see6 = mix(i64(key, offset + 96) ^ SECRET6, i64(key, offset + 104) ^ see6);
                    offset += 112;
                    i -= 112;
                } while (i > 112);

                seed ^= see1;
                see2 ^= see3;
                see4 ^= see5;
                seed ^= see6;
                see2 ^= see4;
                seed ^= see2;
            }
            if (i > 16) {
                seed = mix(i64(key, offset) ^ SECRET2, i64(key, offset + 8) ^ seed);
                if (i > 32) {
                    seed = mix(i64(key, offset + 16) ^ SECRET2, i64(key, offset + 24) ^ seed);
                    if (i > 48) {
                        seed = mix(i64(key, offset + 32) ^ SECRET1, i64(key, offset + 40) ^ seed);
                        if (i > 64) {
                            seed = mix(i64(key, offset + 48) ^ SECRET1, i64(key, offset + 56) ^ seed);
                            if (i > 80) {
                                seed = mix(i64(key, offset + 64) ^ SECRET2, i64(key, offset + 72) ^ seed);
                                if (i > 96) {
                                    seed = mix(i64(key, offset + 80) ^ SECRET1, i64(key, offset + 88) ^ seed);
                                }
                            }
                        }
                    }
                }
            }
            a = i64(key, offset + i - 16) ^ i;
            b = i64(key, offset + i - 8);
        }
        return finish(a, b, seed, i);
    }

    private static long finish(long a, long b, long seed, int i) {
        a ^= SECRET1;
        b ^= seed;
        return mix((a * b) ^ SECRET7, unsignedMultiplyHigh(a, b) ^ SECRET1 ^ i);
    }

    static long mix(long a, long b) {
        return a * b ^ unsignedMultiplyHigh(a, b);
    }

    private static long i64(byte[] key, int p) {
        return (long) LE_LONG.get(key, p);
    }

    private static long u32(byte[] key, int p) {
        return Integer.toUnsignedLong((int) LE_INTEGER.get(key, p));
    }

    private static long i64(ByteBuffer key, int p) {
        return key.getLong(p);
    }

    private static long u32(ByteBuffer key, int p) {
        return Integer.toUnsignedLong(key.getInt(p));
    }
}
