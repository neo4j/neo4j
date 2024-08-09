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
package org.neo4j.internal.batchimport;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.BooleanArray;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class TemporaryIncrementalImportInformation implements Closeable {
    private final FileSystemAbstraction fs;
    private final Path file;

    private final List<Entry> entryList;
    private final Map<String, Entry> entryMap;

    private boolean dirty;

    public TemporaryIncrementalImportInformation(FileSystemAbstraction fs, Path file, Entry... entries)
            throws IOException {
        this.fs = fs;
        this.file = file;
        this.entryList = List.of(entries);
        this.entryMap = entryList.stream().collect(Collectors.toMap(e -> e.key, e -> e));
        readState();
    }

    public <T> T get(String key) {
        Entry entry = entry(key);
        return (T) entry.value;
    }

    public void put(String key, Object value) {
        entry(key).value = value;
        dirty = true;
    }

    private Entry entry(String key) {
        Entry entry = entryMap.get(key);
        Preconditions.checkState(entry != null, "Unknown key %s", key);
        return entry;
    }

    @Override
    public void close() throws IOException {
        write();
    }

    public void write() throws IOException {
        if (dirty) {
            try (var output = new DataOutputStream(fs.openAsOutputStream(file, false))) {
                for (var entry : entryList) {
                    entry.type.write(output, entry.value);
                }
                dirty = false;
            }
        }
    }

    private void readState() throws IOException {
        try (var input = new DataInputStream(fs.openAsInputStream(file))) {
            for (var entry : entryList) {
                entry.value = entry.type.read(input);
            }
        } catch (FileNotFoundException | NoSuchFileException ex) {
            // this is initial state
        }
    }

    private static LongSet readLongs(DataInputStream input) throws IOException {
        final var size = input.readInt();
        if (size == -1) {
            return null;
        }

        final var data = LongSets.mutable.empty();
        for (var i = 0; i < size; i++) {
            data.add(input.readLong());
        }
        return data;
    }

    private static Map<String, Long> readStringLongMap(DataInputStream input) throws IOException {
        final var size = input.readInt();
        if (size == -1) {
            return null;
        }

        final var data = new HashMap<String, Long>();
        for (var i = 0; i < size; i++) {
            String key = null;
            if (input.readBoolean()) {
                key = input.readUTF();
            }
            data.put(key, input.readLong());
        }
        return data;
    }

    private static Map<Long, Map<String, Value>> readLongValuesMap(DataInputStream input) throws IOException {
        final var size = input.readInt();
        if (size == -1) {
            return null;
        }

        final var data = new HashMap<Long, Map<String, Value>>();
        for (var i = 0; i < size; i++) {
            data.put(input.readLong(), readStringValueMap(input));
        }

        return data;
    }

    private static Map<String, Value> readStringValueMap(DataInputStream input) throws IOException {
        final var size = input.readInt();
        final var data = new HashMap<String, Value>();
        for (var i = 0; i < size; i++) {
            data.put(input.readUTF(), readEnum(input, ValueType.VALUES).read(input));
        }

        return data;
    }

    @SafeVarargs
    private static <E extends Enum<E>> E readEnum(DataInputStream input, E... options) throws IOException {
        final var option = (int) input.readByte();
        if (option < 0 || option >= options.length) {
            throw new IOException("Invalid enum option: " + option);
        }

        return options[option];
    }

    private static void writeLongs(DataOutputStream output, LongSet data) throws IOException {
        if (writeStartContainer(output, size(data))) {
            final var iterator = data.longIterator();
            while (iterator.hasNext()) {
                output.writeLong(iterator.next());
            }
        }
    }

    private static void writeStringLongMap(DataOutputStream output, Map<String, Long> data) throws IOException {
        if (writeStartContainer(output, size(data))) {
            for (var entry : data.entrySet()) {
                final var key = entry.getKey();
                output.writeBoolean(key != null);
                if (key != null) {
                    output.writeUTF(key);
                }

                output.writeLong(entry.getValue());
            }
        }
    }

    private static void writeLongValuesMap(DataOutputStream output, Map<Long, Map<String, Value>> data)
            throws IOException {
        if (writeStartContainer(output, size(data))) {
            for (var entry : data.entrySet()) {
                output.writeLong(entry.getKey());

                final var schemaData = entry.getValue();
                output.writeInt(schemaData.size());
                for (var schemaEntry : schemaData.entrySet()) {
                    output.writeUTF(schemaEntry.getKey());
                    ValueType.write(output, schemaEntry.getValue());
                }
            }
        }
    }

    private static boolean writeStartContainer(DataOutputStream output, IntSupplier containerSize) throws IOException {
        if (containerSize == null) {
            output.writeInt(-1);
            return false;
        } else {
            output.writeInt(containerSize.getAsInt());
            return true;
        }
    }

    private static IntSupplier size(LongSet data) {
        return (data == null) ? null : data::size;
    }

    private static IntSupplier size(Map<?, ?> data) {
        return (data == null) ? null : data::size;
    }

    public enum EntryType {
        INT {
            @Override
            void write(DataOutputStream output, Object value) throws IOException {
                output.writeInt((int) value);
            }

            @Override
            Object read(DataInputStream input) throws IOException {
                return input.readInt();
            }

            @Override
            Object defaultValue() {
                return 0;
            }
        },
        LONG {
            @Override
            void write(DataOutputStream output, Object value) throws IOException {
                output.writeLong((long) value);
            }

            @Override
            Object read(DataInputStream input) throws IOException {
                return input.readLong();
            }

            @Override
            Object defaultValue() {
                return 0L;
            }
        },
        LONG_SET {
            @Override
            void write(DataOutputStream output, Object value) throws IOException {
                writeLongs(output, (LongSet) value);
            }

            @Override
            Object read(DataInputStream input) throws IOException {
                return readLongs(input);
            }

            @Override
            Object defaultValue() {
                return LongSets.immutable.empty();
            }
        },
        STRING_LONG_MAP {
            @Override
            void write(DataOutputStream output, Object value) throws IOException {
                writeStringLongMap(output, (Map<String, Long>) value);
            }

            @Override
            Object read(DataInputStream input) throws IOException {
                return readStringLongMap(input);
            }

            @Override
            Object defaultValue() {
                return Collections.emptyMap();
            }
        },
        LONG_VALUES_MAP {
            @Override
            void write(DataOutputStream output, Object value) throws IOException {
                writeLongValuesMap(output, (Map<Long, Map<String, Value>>) value);
            }

            @Override
            Object read(DataInputStream input) throws IOException {
                return readLongValuesMap(input);
            }

            @Override
            Object defaultValue() {
                return Collections.emptyMap();
            }
        };

        abstract void write(DataOutputStream output, Object value) throws IOException;

        abstract Object read(DataInputStream input) throws IOException;

        abstract Object defaultValue();
    }

    // see IndexConfig.validate for allowed value of the index providers map
    private enum ValueType {
        BOOLEAN {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                output.writeBoolean(((BooleanValue) value).booleanValue());
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                return Values.booleanValue(input.readBoolean());
            }
        },
        BOOLEAN_ARRAY {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                final var typedData = (BooleanArray) value;
                output.writeInt(typedData.intSize());
                for (var i = 0; i < typedData.intSize(); i++) {
                    output.writeBoolean(typedData.booleanValue(i));
                }
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                final var array = new boolean[input.readInt()];
                for (var i = 0; i < array.length; i++) {
                    array[i] = input.readBoolean();
                }
                return Values.booleanArray(array);
            }
        },
        SHORT {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                output.writeShort(((ShortValue) value).value());
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                return Values.shortValue(input.readShort());
            }
        },
        SHORT_ARRAY {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                final var typedData = (ShortArray) value;
                output.writeInt(typedData.intSize());
                for (var i = 0; i < typedData.intSize(); i++) {
                    output.writeShort((short) typedData.longValue(i));
                }
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                final var array = new short[input.readInt()];
                for (var i = 0; i < array.length; i++) {
                    array[i] = input.readShort();
                }
                return Values.shortArray(array);
            }
        },
        INT {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                output.writeInt(((IntValue) value).value());
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                return Values.intValue(input.readInt());
            }
        },
        INT_ARRAY {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                final var typedData = (IntArray) value;
                output.writeInt(typedData.intSize());
                for (var i = 0; i < typedData.intSize(); i++) {
                    output.writeInt((int) typedData.longValue(i));
                }
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                final var array = new int[input.readInt()];
                for (var i = 0; i < array.length; i++) {
                    array[i] = input.readInt();
                }
                return Values.intArray(array);
            }
        },
        LONG {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                output.writeLong(((LongValue) value).longValue());
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                return Values.longValue(input.readLong());
            }
        },
        LONG_ARRAY {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                final var typedData = (LongArray) value;
                output.writeInt(typedData.intSize());
                for (var i = 0; i < typedData.intSize(); i++) {
                    output.writeLong(typedData.longValue(i));
                }
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                final var array = new long[input.readInt()];
                for (var i = 0; i < array.length; i++) {
                    array[i] = input.readLong();
                }
                return Values.longArray(array);
            }
        },
        FLOAT {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                output.writeFloat(((FloatValue) value).value());
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                return Values.floatValue(input.readFloat());
            }
        },
        FLOAT_ARRAY {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                final var typedData = (FloatArray) value;
                output.writeInt(typedData.intSize());
                for (var i = 0; i < typedData.intSize(); i++) {
                    output.writeFloat((float) typedData.doubleValue(i));
                }
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                final var array = new float[input.readInt()];
                for (var i = 0; i < array.length; i++) {
                    array[i] = input.readFloat();
                }
                return Values.floatArray(array);
            }
        },
        DOUBLE {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                output.writeDouble(((DoubleValue) value).doubleValue());
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                return Values.doubleValue(input.readDouble());
            }
        },
        DOUBLE_ARRAY {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                final var typedData = (DoubleArray) value;
                output.writeInt(typedData.intSize());
                for (var i = 0; i < typedData.intSize(); i++) {
                    output.writeDouble(typedData.doubleValue(i));
                }
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                final var array = new double[input.readInt()];
                for (var i = 0; i < array.length; i++) {
                    array[i] = input.readDouble();
                }
                return Values.doubleArray(array);
            }
        },
        TEXT {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                output.writeUTF(((TextValue) value).stringValue());
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                return Values.stringValue(input.readUTF());
            }
        },
        TEXT_ARRAY {
            @Override
            protected void writeValue(DataOutputStream output, Value value) throws IOException {
                final var typedData = (TextArray) value;
                output.writeInt(typedData.intSize());
                for (var i = 0; i < typedData.intSize(); i++) {
                    output.writeUTF(typedData.stringValue(i));
                }
            }

            @Override
            Value read(DataInputStream input) throws IOException {
                final var array = new String[input.readInt()];
                for (var i = 0; i < array.length; i++) {
                    array[i] = input.readUTF();
                }
                return Values.stringArray(array);
            }
        };

        private static final ValueType[] VALUES = values();

        private static void write(DataOutputStream output, Value value) throws IOException {
            ValueType type = null;
            if (value instanceof BooleanValue) {
                type = BOOLEAN;
            } else if (value instanceof ShortValue) {
                type = SHORT;
            } else if (value instanceof IntValue) {
                type = INT;
            } else if (value instanceof LongValue) {
                type = LONG;
            } else if (value instanceof FloatValue) {
                type = FLOAT;
            } else if (value instanceof DoubleValue) {
                type = DOUBLE;
            } else if (value instanceof TextValue) {
                type = TEXT;
            } else if (value instanceof BooleanArray) {
                type = BOOLEAN_ARRAY;
            } else if (value instanceof ShortArray) {
                type = SHORT_ARRAY;
            } else if (value instanceof IntArray) {
                type = INT_ARRAY;
            } else if (value instanceof LongArray) {
                type = LONG_ARRAY;
            } else if (value instanceof FloatArray) {
                type = FLOAT_ARRAY;
            } else if (value instanceof DoubleArray) {
                type = DOUBLE_ARRAY;
            } else if (value instanceof TextArray) {
                type = TEXT_ARRAY;
            }

            if (type == null) {
                throw new IOException("Invalid number type found for value: " + value);
            } else {
                output.writeByte(type.ordinal());
                type.writeValue(output, value);
            }
        }

        abstract Value read(DataInputStream input) throws IOException;

        protected abstract void writeValue(DataOutputStream output, Value value) throws IOException;
    }

    public static class Entry {
        private final String key;
        private final EntryType type;
        private Object value;

        public Entry(String key, EntryType type) {
            this(key, type, type.defaultValue());
        }

        public Entry(String key, EntryType type, Object defaultValue) {
            this.key = key;
            this.type = type;
            this.value = defaultValue;
        }
    }
}
