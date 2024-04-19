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
package org.neo4j.internal.batchimport.input;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_OBJECT_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Simple utility for gathering all information about an {@link InputEntityVisitor} and exposing getters
 * for that data. Easier to work with than purely visitor-based implementation in tests.
 */
public class InputEntity implements InputEntityVisitor {
    public static final Object[] NO_PROPERTIES = EMPTY_OBJECT_ARRAY;
    public static final String[] NO_LABELS = EMPTY_STRING_ARRAY;

    private final InputEntityVisitor delegate;

    public InputEntity(InputEntityVisitor delegate) {
        this.delegate = delegate;
        reset();
    }

    public InputEntity() {
        this(NULL);
    }

    public boolean hasPropertyId;
    public long propertyId;
    public boolean hasIntPropertyKeyIds;
    public final List<Object> properties = new ArrayList<>();
    public ByteBuffer encodedProperties;
    public boolean propertiesOffloaded;

    public boolean hasLongId;
    public long longId;
    public Object objectId;
    public Group idGroup;

    public final List<String> labels = new ArrayList<>();
    public boolean hasLabelField;
    public long labelField;

    public boolean hasLongStartId;
    public long longStartId;
    public Object objectStartId;
    public Group startIdGroup;

    public boolean hasLongEndId;
    public long longEndId;
    public Object objectEndId;
    public Group endIdGroup;

    public boolean hasIntType;
    public int intType;
    public String stringType;

    private boolean end;

    @Override
    public boolean propertyId(long nextProp) {
        checkClear();
        hasPropertyId = true;
        propertyId = nextProp;
        return delegate.propertyId(nextProp);
    }

    @Override
    public boolean properties(ByteBuffer properties, boolean offloaded) {
        checkClear();
        encodedProperties = properties;
        propertiesOffloaded = offloaded;
        return delegate.properties(properties, offloaded);
    }

    @Override
    public boolean property(String key, Object value) {
        checkClear();
        properties.add(key);
        properties.add(value);
        return delegate.property(key, value);
    }

    @Override
    public boolean property(int propertyKeyId, Object value) {
        checkClear();
        hasIntPropertyKeyIds = true;
        properties.add(propertyKeyId);
        properties.add(value);
        return delegate.property(propertyKeyId, value);
    }

    @Override
    public boolean id(long id) {
        checkClear();
        hasLongId = true;
        longId = id;
        return delegate.id(id);
    }

    @Override
    public boolean id(Object id, Group group) {
        checkClear();
        objectId = id;
        idGroup = group;
        return delegate.id(id, group);
    }

    @Override
    public boolean id(Object id, Group group, IdSequence idSequence) {
        checkClear();
        objectId = id;
        idGroup = group;
        return delegate.id(id, group, idSequence);
    }

    @Override
    public boolean labels(String[] labels) {
        checkClear();
        Collections.addAll(this.labels, labels);
        return delegate.labels(labels);
    }

    @Override
    public boolean labelField(long labelField) {
        checkClear();
        hasLabelField = true;
        this.labelField = labelField;
        return delegate.labelField(labelField);
    }

    @Override
    public boolean startId(long id) {
        checkClear();
        hasLongStartId = true;
        longStartId = id;
        return delegate.startId(id);
    }

    @Override
    public boolean startId(Object id, Group group) {
        checkClear();
        objectStartId = id;
        startIdGroup = group;
        return delegate.startId(id, group);
    }

    @Override
    public boolean endId(long id) {
        checkClear();
        hasLongEndId = true;
        longEndId = id;
        return delegate.endId(id);
    }

    @Override
    public boolean endId(Object id, Group group) {
        checkClear();
        objectEndId = id;
        endIdGroup = group;
        return delegate.endId(id, group);
    }

    @Override
    public boolean type(int type) {
        checkClear();
        hasIntType = true;
        intType = type;
        return delegate.type(type);
    }

    @Override
    public boolean type(String type) {
        checkClear();
        stringType = type;
        return delegate.type(type);
    }

    @Override
    public void endOfEntity() throws IOException {
        // Mark that the next call to any data method should clear the state
        end = true;
        delegate.endOfEntity();
    }

    public boolean isComplete() {
        return end;
    }

    public String[] labels() {
        return labels.toArray(new String[0]);
    }

    public Object[] properties() {
        return properties.toArray();
    }

    public Map<String, Object> propertiesAsMap() {
        Preconditions.checkState(!hasIntPropertyKeyIds, "This instance doesn't have String keys");
        Map<String, Object> map = new HashMap<>();
        var propertyCount = propertyCount();
        for (int i = 0; i < propertyCount; i++) {
            map.put((String) propertyKey(i), propertyValue(i));
        }
        return map;
    }

    public Iterable<StorageProperty> asStorageProperties(ToIntFunction<String> propertyKeyIdLookup) {
        return () -> new PrefetchingIterator<>() {
            private final int count = propertyCount();
            private int cursor;

            @Override
            protected StorageProperty fetchNextOrNull() {
                if (cursor < count) {
                    int propertyKeyId = propertyKeyIdLookup.applyAsInt((String) propertyKey(cursor));
                    Object valueObject = propertyValue(cursor);
                    cursor++;
                    return new PropertyKeyValue(
                            propertyKeyId, valueObject instanceof Value value ? value : Values.of(valueObject));
                }
                return null;
            }
        };
    }

    public Object id() {
        return hasLongId ? longId : objectId;
    }

    public Object endId() {
        return hasLongEndId ? longEndId : objectEndId;
    }

    public Object startId() {
        return hasLongStartId ? longStartId : objectStartId;
    }

    public Object type() {
        return stringType != null ? stringType : intType;
    }

    public long longStartId(IdMapper.Getter idLookup) {
        return extractNodeId(hasLongStartId, longStartId, objectStartId, startIdGroup, idLookup);
    }

    public long longEndId(IdMapper.Getter idLookup) {
        return extractNodeId(hasLongEndId, longEndId, objectEndId, endIdGroup, idLookup);
    }

    public int intType(ToIntFunction<String> idLookup) {
        if (hasIntType) {
            return intType;
        }
        if (stringType != null) {
            return idLookup.applyAsInt(stringType);
        }
        return -1;
    }

    private long extractNodeId(
            boolean hasLongId, long longId, Object objectId, Group idGroup, IdMapper.Getter idLookup) {
        if (hasLongId) {
            return longId;
        }
        if (objectId != null) {
            return idLookup.get(objectId, idGroup);
        }
        return -1;
    }

    private void checkClear() {
        if (end) {
            reset();
        }
    }

    @Override
    public void reset() {
        end = false;
        hasPropertyId = false;
        propertyId = -1;
        hasIntPropertyKeyIds = false;
        properties.clear();
        encodedProperties = null;
        propertiesOffloaded = false;
        hasLongId = false;
        longId = -1;
        objectId = null;
        idGroup = null;
        labels.clear();
        hasLabelField = false;
        labelField = -1;
        hasLongStartId = false;
        longStartId = -1;
        objectStartId = null;
        startIdGroup = null;
        hasLongEndId = false;
        longEndId = -1;
        objectEndId = null;
        endIdGroup = null;
        hasIntType = false;
        intType = -1;
        stringType = null;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    public int propertyCount() {
        return properties.size() / 2;
    }

    public Object propertyKey(int i) {
        return properties.get(i * 2);
    }

    public Object propertyValue(int i) {
        return properties.get(i * 2 + 1);
    }

    public void replayOnto(InputEntityVisitor visitor) throws IOException {
        // properties
        if (hasPropertyId) {
            visitor.propertyId(propertyId);
        } else if (!properties.isEmpty()) {
            int propertyCount = propertyCount();
            for (int i = 0; i < propertyCount; i++) {
                if (hasIntPropertyKeyIds) {
                    visitor.property((Integer) propertyKey(i), propertyValue(i));
                } else {
                    visitor.property((String) propertyKey(i), propertyValue(i));
                }
            }
        }

        // id
        if (hasLongId) {
            visitor.id(longId);
        } else if (objectId != null) {
            visitor.id(objectId, idGroup);
        }

        // labels
        if (hasLabelField) {
            visitor.labelField(labelField);
        } else if (!labels.isEmpty()) {
            visitor.labels(labels.toArray(new String[0]));
        }

        // start id
        if (hasLongStartId) {
            visitor.startId(longStartId);
        } else if (objectStartId != null) {
            visitor.startId(objectStartId, startIdGroup);
        }

        // end id
        if (hasLongEndId) {
            visitor.endId(longEndId);
        } else if (objectEndId != null) {
            visitor.endId(objectEndId, endIdGroup);
        }

        // type
        if (hasIntType) {
            visitor.type(intType);
        } else if (stringType != null) {
            visitor.type(stringType);
        }

        // all done
        visitor.endOfEntity();
    }
}
