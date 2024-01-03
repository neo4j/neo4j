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
package org.neo4j.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.primitive.LongList;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;

/**
 * Like a {@link Random} but guarantees to include the seed with the test failure, which helps
 * greatly in debugging.
 *
 * To reproduce same random sequence when debugging use the seed included in the test failure.
 * If {@link Lifecycle} is {@link Lifecycle#PER_METHOD} (default), place {@link Seed} annotation on method.
 * If {@link Lifecycle} is {@link Lifecycle#PER_CLASS} place {@link Seed} annotation on class. Note that to
 * reproduce the exact execution the test order must be the same since the same random instance is used across all tests.
 * To ensure this, it's recommended to use a {@link TestMethodOrder}, for example with {@link MethodOrderer.MethodName}.
 *
 * Available methods directly on this class include those found in {@link RandomValues} and the basic ones in {@link Random}.
 */
public class RandomSupport {
    private long globalSeed;
    private long seed;
    private boolean hasGlobalSeed;
    private final Random random = new Random();
    private RandomValues randoms;

    private RandomValues.Configuration config = RandomValues.DEFAULT_CONFIGURATION;

    public RandomSupport withConfiguration(RandomValues.Configuration config) {
        this.config = config;
        return this;
    }

    public RandomSupport withSeedForAllTests(long seed) {
        hasGlobalSeed = true;
        this.globalSeed = seed;
        return this;
    }

    // ============================
    // Methods from Random
    // ============================

    public byte[] nextBytes(byte[] bytes) {
        random.nextBytes(bytes);
        return bytes;
    }

    public boolean nextBoolean() {
        return random.nextBoolean();
    }

    public double nextDouble() {
        return random.nextDouble();
    }

    public DoubleStream doubles(int dimension, double minValue, double maxValue) {
        return random.doubles(dimension, minValue, maxValue);
    }

    public float nextFloat() {
        return random.nextFloat();
    }

    public int nextInt() {
        return random.nextInt();
    }

    /**
     * @param bound – the upper bound (exclusive)
     */
    public int nextInt(int bound) {
        return random.nextInt(bound);
    }

    /**
     * @param origin - the origin (inclusive)
     * @param bound – the upper bound (exclusive)
     */
    public int nextInt(int origin, int bound) {
        return random.nextInt(bound - origin) + origin;
    }

    public IntStream ints(long streamSize) {
        return random.ints(streamSize);
    }

    public IntStream ints(long streamSize, int randomNumberOrigin, int randomNumberBound) {
        return random.ints(streamSize, randomNumberOrigin, randomNumberBound);
    }

    public double nextGaussian() {
        return random.nextGaussian();
    }

    public long nextLong() {
        return random.nextLong();
    }

    /**
     * @param bound – the upper bound (exclusive)
     */
    public long nextLong(long bound) {
        var randomLong = nextLong();
        if (randomLong == Long.MIN_VALUE) {
            randomLong = 66;
        }
        return Math.abs(randomLong) % bound;
    }

    /**
     * @param origin - the origin (inclusive)
     * @param bound – the upper bound (exclusive)
     */
    public long nextLong(long origin, long bound) {
        return nextLong(bound - origin) + origin;
    }

    /**
     * Runs a {@link Runnable} with specified probability.
     *
     * @param probability
     * @param runnable
     * @return if run
     */
    public boolean withProbability(double probability, Runnable runnable) {
        final var run = nextDouble() < probability;
        if (run) {
            runnable.run();
        }
        return run;
    }

    // ============================
    // Methods from RandomValues
    // ============================

    public int intBetween(int min, int max) {
        return randoms.intBetween(min, max);
    }

    public String nextString() {
        return nextTextValue().stringValue();
    }

    public TextValue nextTextValue() {
        return randoms.nextTextValue();
    }

    public String nextAlphaNumericString() {
        return nextAlphaNumericTextValue().stringValue();
    }

    public String nextAsciiString() {
        return nextAsciiTextValue().stringValue();
    }

    private TextValue nextAsciiTextValue() {
        return randoms.nextAsciiTextValue();
    }

    public TextValue nextAlphaNumericTextValue() {
        return randoms.nextAlphaNumericTextValue();
    }

    public String nextAlphaNumericString(int minLength, int maxLength) {
        return nextAlphaNumericTextValue(minLength, maxLength).stringValue();
    }

    public TextValue nextAlphaNumericTextValue(int minLength, int maxLength) {
        return randoms.nextAlphaNumericTextValue(minLength, maxLength);
    }

    public TextValue nextBasicMultilingualPlaneTextValue() {
        return randoms.nextBasicMultilingualPlaneTextValue();
    }

    public String nextBasicMultilingualPlaneString() {
        return nextBasicMultilingualPlaneTextValue().stringValue();
    }

    public <T> T[] selection(T[] among, int min, int max, boolean allowDuplicates) {
        return randoms.selection(among, min, max, allowDuplicates);
    }

    public int[] selection(int[] among, int min, int max, boolean allowDuplicates) {
        return randoms.selection(among, min, max, allowDuplicates);
    }

    public long[] selection(long[] among, int min, int max, boolean allowDuplicates) {
        return randoms.selection(among, min, max, allowDuplicates);
    }

    @SafeVarargs
    public final <T> T among(T... among) {
        return randoms.among(among);
    }

    public long among(long[] among) {
        return randoms.among(among);
    }

    public int among(int[] among) {
        return randoms.among(among);
    }

    public <T> T among(List<T> among) {
        return randoms.among(among);
    }

    public <T> void among(List<T> among, Consumer<T> action) {
        randoms.among(among, action);
    }

    public long among(LongList among) {
        return randoms.among(among);
    }

    public <T> T among(RichIterable<T> among) {
        int offset = nextInt(among.size());
        final var iterator = among.iterator();
        while (offset-- > 0) {
            iterator.next();
        }
        return iterator.next();
    }

    public Object nextValueAsObject() {
        return randoms.nextValue().asObject();
    }

    public Value nextValue() {
        return randoms.nextValue();
    }

    public Value nextValue(ValueType type) {
        return randoms.nextValueOfType(type);
    }

    public ArrayValue nextArray() {
        return randoms.nextArray();
    }

    // ============================
    // Other utility methods
    // ============================

    public void reset() {
        random.setSeed(seed);
        randoms = RandomValues.create(random, config);
    }

    public long seed() {
        return seed;
    }

    public Random random() {
        return random;
    }

    public RandomValues randomValues() {
        return randoms;
    }

    public void setSeed(long seed) {
        this.seed = seed;
        reset();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.TYPE})
    public @interface Seed {
        long value();
    }
}
