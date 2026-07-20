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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Simple utility for gathering all information about an {@link InputEntityVisitor} and exposing getters
 * for that data. Easier to work with than purely visitor-based implementation in tests.
 */
public class InputEntity implements InputEntityVisitor {
    public static final String[] NO_LABELS = EMPTY_STRING_ARRAY;
    public static final int NULL_ID = -1;

    public InputEntity() {
        reset();
    }

    public boolean hasPropertyId;
    public long propertyId;
    public boolean hasIntPropertyKeyIds;
    public final List<Property> properties = FastList.newList();
    public final List<String> removedProperties = new ArrayList<>();
    public final MutableIntList intRemovedProperties = IntLists.mutable.empty();
    public ByteBuffer encodedProperties;
    public boolean propertiesOffloaded;

    public boolean hasLongId;
    public long longId;
    public IdSequence idSequence;
    public Object objectId;
    public Group idGroup;

    public final List<String> labels = new ArrayList<>();
    public final List<String> removedLabels = new ArrayList<>();
    public final MutableIntList intLabels = IntLists.mutable.empty();
    public final MutableIntList intRemovedLabels = IntLists.mutable.empty();
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

    public String sourceDescription;
    public long lineNumber;

    public boolean hasIntType;
    public int intType;
    public String stringType;

    public ApplicationMode applicationMode;

    private boolean end;

    @Override
    public boolean propertyId(long nextProp) {
        checkClear();
        hasPropertyId = true;
        propertyId = nextProp;
        return true;
    }

    @Override
    public boolean properties(ByteBuffer properties, boolean offloaded) {
        checkClear();
        encodedProperties = properties;
        propertiesOffloaded = offloaded;
        return true;
    }

    @Override
    public boolean property(String key, Object value, boolean identifier) {
        assert value != Values.NO_VALUE;
        checkClear();
        properties.add(new Property(key, NO_TOKEN, value, identifier));
        return true;
    }

    @Override
    public boolean property(int propertyKeyId, Object value, boolean identifier) {
        assert value != Values.NO_VALUE;
        checkClear();
        hasIntPropertyKeyIds = true;
        properties.add(new Property(null, propertyKeyId, value, identifier));
        return true;
    }

    @Override
    public boolean removedProperties(String[] keys) {
        checkClear();
        Collections.addAll(removedProperties, keys);
        return true;
    }

    @Override
    public boolean removedProperties(int[] keys) {
        checkClear();
        intRemovedProperties.addAll(keys);
        return true;
    }

    @Override
    public boolean id(long id) {
        checkClear();
        hasLongId = true;
        longId = id;
        return true;
    }

    @Override
    public boolean id(Object id, Group group) {
        checkClear();
        objectId = id;
        idGroup = group;
        return true;
    }

    @Override
    public boolean id(Object id, Group group, IdSequence idSequence) {
        this.idSequence = idSequence;
        checkClear();
        objectId = id;
        idGroup = group;
        return true;
    }

    @Override
    public boolean labels(String[] labels) {
        checkClear();
        Collections.addAll(this.labels, labels);
        return true;
    }

    @Override
    public boolean labels(int[] labels) {
        checkClear();
        intLabels.addAll(labels);
        return true;
    }

    @Override
    public boolean removedLabels(String[] labels) {
        checkClear();
        Collections.addAll(this.removedLabels, labels);
        return true;
    }

    @Override
    public boolean removedLabels(int[] labels) {
        checkClear();
        intRemovedLabels.addAll(labels);
        return true;
    }

    @Override
    public boolean labelField(long labelField) {
        checkClear();
        hasLabelField = true;
        this.labelField = labelField;
        return true;
    }

    @Override
    public boolean startId(long id) {
        checkClear();
        hasLongStartId = true;
        longStartId = id;
        return true;
    }

    @Override
    public boolean startId(Object id, Group group) {
        checkClear();
        objectStartId = id;
        startIdGroup = group;
        return true;
    }

    @Override
    public boolean endId(long id) {
        checkClear();
        hasLongEndId = true;
        longEndId = id;
        return true;
    }

    @Override
    public boolean endId(Object id, Group group) {
        checkClear();
        objectEndId = id;
        endIdGroup = group;
        return true;
    }

    @Override
    public boolean sourceDescription(String source) {
        sourceDescription = source;
        return true;
    }

    @Override
    public boolean lineNumber(long line) {
        lineNumber = line;
        return true;
    }

    @Override
    public boolean type(int type) {
        checkClear();
        hasIntType = true;
        intType = type;
        return true;
    }

    @Override
    public boolean type(String type) {
        checkClear();
        stringType = type;
        return true;
    }

    @Override
    public boolean applicationMode(ApplicationMode mode) {
        checkClear();
        applicationMode = mode;
        return true;
    }

    public ApplicationMode applicationMode() {
        return applicationMode != null ? applicationMode : ApplicationMode.CREATE;
    }

    @Override
    public void endOfEntity() throws IOException {
        // Mark that the next call to any data method should clear the state
        end = true;
    }

    public boolean isComplete() {
        return end;
    }

    public String[] labels() {
        return labels.toArray(new String[0]);
    }

    private IntSet tokenIds(IntList ids, List<String> names, ToIntFunction<String> nameToIdLookup) {
        if (ids.isEmpty() && names.isEmpty()) {
            return IntSets.immutable.empty();
        }

        MutableIntSet result = IntSets.mutable.empty();
        result.addAll(ids);
        for (String name : names) {
            int id = nameToIdLookup.applyAsInt(name);
            if (id != NO_TOKEN) {
                result.add(id);
            }
        }
        return result;
    }

    public IntSet labelIds(ToIntFunction<String> nameToIdLookup) {
        return tokenIds(intLabels, labels, nameToIdLookup);
    }

    public IntSet removedLabelIds(ToIntFunction<String> nameToIdLookup) {
        return tokenIds(intRemovedLabels, removedLabels, nameToIdLookup);
    }

    public IntSet removedPropertyIds(ToIntFunction<String> nameToIdLookup) {
        return tokenIds(intRemovedProperties, removedProperties, nameToIdLookup);
    }

    public Map<String, Object> propertiesAsMap() {
        Preconditions.checkState(!hasIntPropertyKeyIds, "This instance doesn't have String keys");
        Map<String, Object> map = new HashMap<>();
        for (var p : properties) {
            map.put(p.keyName(), p.value);
        }
        return map;
    }

    public Map<String, Value> propertiesAsValueMap() {
        Preconditions.checkState(!hasIntPropertyKeyIds, "This instance doesn't have String keys");
        Map<String, Value> map = new HashMap<>();
        for (var p : properties) {
            map.put(p.keyName(), p.asValue());
        }
        return map;
    }

    public Property getProperty(String key) {
        for (var p : properties) {
            if (key.equals(p.keyName())) {
                return p;
            }
        }
        return null;
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

    public String sourceDescription() {
        return sourceDescription;
    }

    public long lineNumber() {
        return lineNumber;
    }

    public Object type() {
        return stringType != null ? stringType : intType;
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
        propertyId = NULL_ID;
        hasIntPropertyKeyIds = false;
        properties.clear();
        removedProperties.clear();
        intRemovedProperties.clear();
        encodedProperties = null;
        propertiesOffloaded = false;
        hasLongId = false;
        longId = NULL_ID;
        idSequence = null;
        objectId = null;
        idGroup = null;
        labels.clear();
        removedLabels.clear();
        hasLabelField = false;
        labelField = NULL_ID;
        hasLongStartId = false;
        longStartId = NULL_ID;
        objectStartId = null;
        startIdGroup = null;
        hasLongEndId = false;
        longEndId = NULL_ID;
        objectEndId = null;
        endIdGroup = null;
        hasIntType = false;
        intType = NULL_ID;
        stringType = null;
        applicationMode = null;
        intLabels.clear();
        intRemovedLabels.clear();
        sourceDescription = null;
        lineNumber = 0;
    }

    public void replayOnto(InputEntityVisitor visitor) throws IOException {
        if (applicationMode != null) {
            visitor.applicationMode(applicationMode);
        }

        // properties
        if (hasPropertyId) {
            visitor.propertyId(propertyId);
        } else if (!properties.isEmpty()) {
            for (var p : properties) {
                if (p.hasKeyId()) {
                    visitor.property(p.keyId(), p.value, p.identifier);
                } else {
                    visitor.property(p.keyName(), p.value, p.identifier);
                }
            }
        }
        if (!removedProperties.isEmpty()) {
            visitor.removedProperties(removedProperties.toArray(new String[0]));
        }
        if (!intRemovedProperties.isEmpty()) {
            visitor.removedProperties(intRemovedProperties.toArray());
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
        } else {
            if (!labels.isEmpty()) {
                visitor.labels(labels.toArray(new String[0]));
            }
            if (!removedLabels.isEmpty()) {
                visitor.removedLabels(removedLabels.toArray(new String[0]));
            }
            if (!intLabels.isEmpty()) {
                visitor.labels(intLabels.toArray());
            }
            if (!intRemovedLabels.isEmpty()) {
                visitor.removedLabels(intRemovedLabels.toArray());
            }
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

    public void updateWithDataFrom(InputEntity increment) {
        // properties
        for (var property : increment.properties) {
            properties.stream()
                    .filter(p -> p.keyName().equals(property.keyName()))
                    .findFirst()
                    .ifPresent(properties::remove);
            properties.add(property);
        }

        // removed properties
        for (var key : increment.removedProperties) {
            properties.stream().filter(p -> key.equals(p.keyName())).findFirst().ifPresent(properties::remove);
        }

        // labels
        increment.labels.stream().filter(l -> !labels.contains(l)).forEach(labels::add);

        // removed labels
        increment.removedLabels.forEach(labels::remove);
    }

    public record Property(String keyName, int keyId, Object value, boolean identifier) {
        public Value asValue() {
            return value instanceof Value v ? v : Values.of(value);
        }

        @Override
        public String keyName() {
            assert keyName != null : "The key name isn't present";
            return keyName;
        }

        @Override
        public int keyId() {
            assert keyId != NO_TOKEN : "The key id isn't present";
            return keyId;
        }

        public boolean hasKeyId() {
            return keyId != NO_TOKEN;
        }
    }
}
