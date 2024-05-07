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
package org.neo4j.storageengine.api;

import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.util.Arrays;
import java.util.function.IntPredicate;

/**
 * Specifies criteria for which properties to select.
 */
public abstract class PropertySelection {
    public static final int UNKNOWN_NUMBER_OF_KEYS = -1;

    private final boolean keysOnly;

    protected PropertySelection(boolean keysOnly) {
        this.keysOnly = keysOnly;
    }

    /**
     * @return {@code true} if this selection limits which keys will be selected, otherwise {@code false} if all will be selected.
     */
    public abstract boolean isLimited();

    /**
     * @return the number of keys in this selection. Selections that are not discrete returns {@code -1}.
     */
    public abstract int numberOfKeys();

    public boolean isEmpty() {
        return numberOfKeys() == 0;
    }

    /**
     * @param index the selection index. A selection can have multiple keys.
     * @return the key for the given selection index.
     */
    public abstract int key(int index);

    /**
     * @param key the key to tests whether it fits the criteria of this selection.
     * @return {@code true} if the given {@code key} is part of this selection, otherwise {@code false}.
     */
    public abstract boolean test(int key);

    /**
     * A hint that the creator of this selection isn't interested in the actual values, only the existence of the keys.
     * @return {@code true} if only keys will be extracted where this selection is used, otherwise {@code false} if also values will be extracted.
     */
    public boolean isKeysOnly() {
        return keysOnly;
    }

    /**
     * @return lowest key in this selection.
     */
    public abstract int lowestKey();

    /**
     * @return highest key in this selection.
     */
    public abstract int highestKey();

    /**
     * @param filter a predicate such that for any key where {@link IntPredicate#test(int)} returns true will be excluded.
     * @return a {@link PropertySelection} instance that excludes keys from the existing set of keys.
     */
    public abstract PropertySelection excluding(IntPredicate filter);

    @Override
    public String toString() {
        return String.format("Property%sSelection", keysOnly ? "Key" : "");
    }

    /**
     * Creates a {@link PropertySelection} with its single criterion based on the given {@code key}.
     *
     * @param key a single key that should be selected.
     * @return a {@link PropertySelection} instance with the given {@code key} as its criterion.
     */
    public static PropertySelection selection(int key) {
        return SingleKey.singleKey(false, key);
    }

    /**
     * Creates a {@link PropertySelection} with its criteria based on the given {@code keys}.
     *
     * @param keys one or more keys that should be part of the created selection.
     * @return a {@link PropertySelection} instance with the given {@code keys} as its criteria.
     */
    public static PropertySelection selection(int... keys) {
        return selection(false, keys);
    }

    /**
     * Creates a {@link PropertySelection} with its criteria based on the given {@code keys}.
     * This selection will hint that only the keys are interesting, not the values.
     *
     * @param keys one or more keys that should be part of the created selection.
     * @return a {@link PropertySelection} instance with the given {@code keys} as its criteria.
     */
    public static PropertySelection onlyKeysSelection(int... keys) {
        return selection(true, keys);
    }

    private static PropertySelection selection(boolean keysOnly, int[] keys) {
        if (keys == null) {
            return keysOnly ? ALL_PROPERTY_KEYS : ALL_PROPERTIES;
        }
        if (keys.length == 0) {
            return NO_PROPERTIES;
        }
        if (keys.length == 1) {
            int key = keys[0];
            return key == NO_TOKEN ? NO_PROPERTIES : SingleKey.singleKey(keysOnly, key);
        }
        return new MultipleKeys(keysOnly, keys);
    }

    private static class SingleKey extends PropertySelection {
        private static final int LOW_ID_THRESHOLD = 128;
        private static final PropertySelection[] SINGLE_LOW_ID_SELECTIONS = new PropertySelection[LOW_ID_THRESHOLD];
        private static final PropertySelection[] SINGLE_LOW_ID_KEY_SELECTIONS = new PropertySelection[LOW_ID_THRESHOLD];

        static {
            for (int key = 0; key < SINGLE_LOW_ID_SELECTIONS.length; key++) {
                SINGLE_LOW_ID_SELECTIONS[key] = new PropertySelection.SingleKey(false, key);
                SINGLE_LOW_ID_KEY_SELECTIONS[key] = new PropertySelection.SingleKey(true, key);
            }
        }

        private static PropertySelection singleKey(boolean keysOnly, int key) {
            if (key < LOW_ID_THRESHOLD && key >= 0) {
                return keysOnly ? SINGLE_LOW_ID_KEY_SELECTIONS[key] : SINGLE_LOW_ID_SELECTIONS[key];
            }
            return new SingleKey(keysOnly, key);
        }

        private final int key;

        private SingleKey(boolean keysOnly, int key) {
            super(keysOnly);
            this.key = key;
        }

        @Override
        public boolean isLimited() {
            return true;
        }

        @Override
        public int numberOfKeys() {
            return 1;
        }

        @Override
        public int key(int index) {
            assert index == 0;
            return key;
        }

        @Override
        public boolean test(int key) {
            return this.key == key;
        }

        @Override
        public int lowestKey() {
            return key;
        }

        @Override
        public int highestKey() {
            return key;
        }

        @Override
        public PropertySelection excluding(IntPredicate filter) {
            return filter.test(key) ? NO_PROPERTIES : this;
        }

        @Override
        public String toString() {
            return super.toString() + "[" + key + "]";
        }
    }

    private static class MultipleKeys extends PropertySelection {
        private final int[] keys;

        private MultipleKeys(boolean keysOnly, int[] keys) {
            super(keysOnly);
            this.keys = cloneAndCleanUp(keys);
        }

        /**
         * Clones the {@code suppliedKeys} for safety, since it's coming from "user" call site.
         * The cloned keys are sorted and also any -1 values (likely coming from name->id lookup
         * returning {@link org.neo4j.token.api.TokenConstants#NO_TOKEN}.
         */
        private int[] cloneAndCleanUp(int[] suppliedKeys) {
            var keys = suppliedKeys.clone();
            Arrays.sort(keys);
            if (keys[0] == NO_TOKEN) {
                int start = 1;
                while (start < keys.length && keys[start] == NO_TOKEN) {
                    start++;
                }
                keys = Arrays.copyOfRange(keys, start, keys.length);
            }
            return keys;
        }

        @Override
        public boolean isLimited() {
            return true;
        }

        @Override
        public int numberOfKeys() {
            return keys.length;
        }

        @Override
        public int key(int index) {
            assert index >= 0 && index < keys.length;
            return keys[index];
        }

        @Override
        public boolean test(int key) {
            for (int k : keys) {
                if (k == key) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int lowestKey() {
            return keys[0];
        }

        @Override
        public int highestKey() {
            return keys[keys.length - 1];
        }

        @Override
        public PropertySelection excluding(IntPredicate filter) {
            var newKeys = new int[keys.length];
            int t = 0;
            for (int key : keys) {
                if (!filter.test(key)) {
                    newKeys[t++] = key;
                }
            }
            if (t == keys.length) {
                return this;
            }
            return PropertySelection.selection(isKeysOnly(), Arrays.copyOf(newKeys, t));
        }

        @Override
        public String toString() {
            return super.toString() + "[" + Arrays.toString(keys) + "]";
        }
    }

    private static class AllExcept extends PropertySelection {
        private final IntPredicate excluded;

        AllExcept(boolean keysOnly, IntPredicate excluded) {
            super(keysOnly);
            this.excluded = excluded;
        }

        @Override
        public boolean isLimited() {
            return true;
        }

        @Override
        public int numberOfKeys() {
            return UNKNOWN_NUMBER_OF_KEYS;
        }

        @Override
        public int key(int index) {
            throw new IllegalStateException("This selection has no discrete number of keys");
        }

        @Override
        public boolean test(int key) {
            return !excluded.test(key);
        }

        @Override
        public int lowestKey() {
            return 0;
        }

        @Override
        public int highestKey() {
            return Integer.MAX_VALUE;
        }

        @Override
        public PropertySelection excluding(IntPredicate filter) {
            return new AllExcept(isKeysOnly(), excluded.or(filter));
        }
    }

    public static final PropertySelection ALL_PROPERTIES = allProperties(false);
    public static final PropertySelection ALL_PROPERTY_KEYS = allProperties(true);
    public static final PropertySelection NO_PROPERTIES = new PropertySelection(true) {
        @Override
        public boolean isLimited() {
            return true;
        }

        @Override
        public int numberOfKeys() {
            return 0;
        }

        @Override
        public int key(int index) {
            throw new IllegalStateException("This selection has no keys");
        }

        @Override
        public boolean test(int key) {
            return false;
        }

        @Override
        public int lowestKey() {
            return -1;
        }

        @Override
        public int highestKey() {
            return -1;
        }

        @Override
        public PropertySelection excluding(IntPredicate filter) {
            return this;
        }
    };

    private static PropertySelection allProperties(boolean keysOnly) {
        return new PropertySelection(keysOnly) {
            @Override
            public boolean isLimited() {
                return false;
            }

            @Override
            public int numberOfKeys() {
                return UNKNOWN_NUMBER_OF_KEYS;
            }

            @Override
            public int key(int index) {
                return NO_TOKEN;
            }

            @Override
            public boolean test(int key) {
                return true;
            }

            @Override
            public int lowestKey() {
                return 0;
            }

            @Override
            public int highestKey() {
                return Integer.MAX_VALUE;
            }

            @Override
            public PropertySelection excluding(IntPredicate filter) {
                return new AllExcept(isKeysOnly(), filter);
            }

            @Override
            public String toString() {
                return super.toString() + "[*]";
            }
        };
    }
}
