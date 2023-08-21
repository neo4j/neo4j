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
package org.neo4j.kernel.impl.index.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
@TestInstance(PER_CLASS)
public class PointKeyStateTest {
    private final IndexSpecificSpaceFillingCurveSettings noSpecificIndexSettings =
            IndexSpecificSpaceFillingCurveSettings.fromConfig(Config.defaults());

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @MethodSource("valueGenerators")
    void readWhatIsWritten(ValueGenerator valueGenerator) {
        // Given
        PageCursor cursor = newPageCursor();
        PointKey writeState = newKeyState();
        Value value = valueGenerator.next();
        int offset = cursor.getOffset();

        // When
        writeState.initialize(123);
        writeState.writeValue(-1, value, NEUTRAL);
        writeState.writeToCursor(cursor);

        // Then
        PointKey readState = newKeyState();
        int size = writeState.size();
        cursor.setOffset(offset);
        readState.readFromCursor(cursor, size);
        assertEquals(0, readState.compareValueTo(writeState));
        Value readValue = readState.asValues()[0];
        assertEquals(value, readValue);
        assertEquals(123, readState.getEntityId());
    }

    @ParameterizedTest
    @MethodSource("valueGenerators")
    void copyShouldCopy(ValueGenerator valueGenerator) {
        // Given
        PointKey from = newKeyState();
        Value value = valueGenerator.next();
        from.initialize(123);
        from.writeValue(-1, value, NEUTRAL);
        PointKey to = pointKeyStateWithSomePreviousState(valueGenerator);

        // When
        to.copyFrom(from);

        // Then
        assertEquals(0, from.compareValueTo(to));
        assertEquals(123, to.getEntityId());
    }

    @Test
    void copyShouldCopyExtremeValues() {
        // Given
        PointKey extreme = newKeyState();
        PointKey copy = newKeyState();

        extreme.initValueAsLowest(-1, null);
        copy.copyFrom(extreme);
        assertEquals(0, extreme.compareValueTo(copy));
        extreme.initValueAsHighest(-1, null);
        copy.copyFrom(extreme);
        assertEquals(0, extreme.compareValueTo(copy));
    }

    @Test
    void comparePointsMustOnlyReturnZeroForEqualPoints() {
        PointValue firstPoint = random.randomValues().nextPointValue();
        PointValue equalPoint = Values.point(firstPoint);
        CoordinateReferenceSystem crs = firstPoint.getCoordinateReferenceSystem();
        SpaceFillingCurve curve = noSpecificIndexSettings.forCrs(crs);
        Long spaceFillingCurveValue = curve.derivedValueFor(firstPoint.coordinate());
        PointValue centerPoint = Values.pointValue(crs, curve.centerPointFor(spaceFillingCurveValue));

        PointKey firstKey = newKeyState();
        firstKey.writeValue(-1, firstPoint, NEUTRAL);
        PointKey equalKey = newKeyState();
        equalKey.writeValue(-1, equalPoint, NEUTRAL);
        PointKey centerKey = newKeyState();
        centerKey.writeValue(-1, centerPoint, NEUTRAL);

        assertEquals(0, firstKey.compareValueTo(equalKey));
        assertEquals(firstPoint.compareTo(centerPoint) != 0, firstKey.compareValueTo(centerKey) != 0);
    }

    @ParameterizedTest
    @MethodSource("valueGenerators")
    void mustReportCorrectSize(ValueGenerator valueGenerator) {
        // Given
        PageCursor cursor = newPageCursor();
        Value value = valueGenerator.next();
        PointKey state = newKeyState();
        state.initialize(123);
        state.writeValue(-1, value, NEUTRAL);
        int offsetBefore = cursor.getOffset();

        // When
        int reportedSize = state.size();
        state.writeToCursor(cursor);
        int offsetAfter = cursor.getOffset();

        // Then
        int actualSize = offsetAfter - offsetBefore;
        assertEquals(reportedSize, actualSize);
    }

    @ParameterizedTest
    @MethodSource("valueGenerators")
    void mustProduceValidMinimalSplittersWhenValuesAreNotEqual(ValueGenerator valueGenerator) {
        PointKey key1 = newKeyState();
        Value value1 = valueGenerator.next();
        key1.writeValue(-1, value1, NEUTRAL);
        PointKey key2;
        do {
            key2 = newKeyState();
            Value value2 = valueGenerator.next();
            key2.writeValue(-1, value2, NEUTRAL);
        } while (key1.compareValueTo(key2) == 0);

        List<PointKey> keys =
                Stream.of(key1, key2).sorted(PointKey::compareValueTo).collect(Collectors.toList());

        PointKey splitter = newKeyState();
        newLayout().minimalSplitter(keys.get(0), keys.get(1), splitter);
        assertEquals(0, keys.get(1).compareValueTo(splitter));
    }

    @ParameterizedTest
    @MethodSource("valueGenerators")
    void mustProduceValidMinimalSplittersWhenValuesAreEqual(ValueGenerator valueGenerator) {
        // Given
        Value value = valueGenerator.next();
        PointLayout layout = newLayout();
        PointKey left = layout.newKey();
        PointKey right = layout.newKey();
        PointKey minimalSplitter = layout.newKey();

        // keys with same value but different entityId
        left.initialize(123);
        left.initFromValue(-1, value, NEUTRAL);
        right.initialize(234);
        right.initFromValue(-1, value, NEUTRAL);

        // When creating minimal splitter
        layout.minimalSplitter(left, right, minimalSplitter);

        // Then that minimal splitter need to correctly divide left and right
        assertTrue(layout.compare(left, minimalSplitter) < 0);
        assertTrue(layout.compare(minimalSplitter, right) <= 0);
        assertEquals(234, minimalSplitter.getEntityId());
    }

    @ParameterizedTest
    @MethodSource("valueGenerators")
    void minimalSplitterShouldRemoveEntityIdIfPossible(ValueGenerator valueGenerator) {
        PointKey key1 = newKeyState();
        Value value1 = valueGenerator.next();
        key1.initialize(123);
        key1.writeValue(-1, value1, NEUTRAL);
        PointKey key2;
        do {
            key2 = newKeyState();
            Value value2 = valueGenerator.next();
            key1.initialize(234);
            key2.writeValue(-1, value2, NEUTRAL);
        } while (key1.compareValueTo(key2) == 0);

        List<PointKey> keys =
                Stream.of(key1, key2).sorted(PointKey::compareValueTo).collect(Collectors.toList());

        PointKey splitter = newKeyState();
        newLayout().minimalSplitter(keys.get(0), keys.get(1), splitter);

        // Then that minimal splitter should have entity id shaved off
        assertEquals(NativeIndexKey.NO_ENTITY_ID, splitter.getEntityId());
    }

    private PointKey pointKeyStateWithSomePreviousState(ValueGenerator valueGenerator) {
        PointKey to = newKeyState();
        if (random.nextBoolean()) {
            // Previous value
            to.setEntityId(random.nextLong(1000000));
            NativeIndexKey.Inclusion inclusion = random.among(NativeIndexKey.Inclusion.values());
            Value value = valueGenerator.next();
            to.writeValue(-1, value, inclusion);
        }
        // No previous state
        return to;
    }

    private Stream<ValueGenerator> valueGenerators() {
        return Stream.of(
                new ValueGenerator(() -> random.randomValues().nextPointValue(), "point"),
                new ValueGenerator(() -> random.randomValues().nextGeographicPoint(), "geographic point"),
                new ValueGenerator(() -> random.randomValues().nextGeographic3DPoint(), "geographic point 3D"),
                new ValueGenerator(() -> random.randomValues().nextCartesianPoint(), "cartesian point"),
                new ValueGenerator(() -> random.randomValues().nextCartesian3DPoint(), "cartesian point 3D"));
    }

    private static PageCursor newPageCursor() {
        return ByteArrayPageCursor.wrap(PageCache.PAGE_SIZE);
    }

    private PointKey newKeyState() {
        return new PointKey(noSpecificIndexSettings);
    }

    private PointLayout newLayout() {
        return new PointLayout(noSpecificIndexSettings);
    }

    private static class ValueGenerator {
        private final Supplier<Value> valueSupplier;
        private final String description;

        ValueGenerator(Supplier<Value> valueSupplier, String description) {
            this.valueSupplier = valueSupplier;
            this.description = description;
        }

        Value next() {
            return valueSupplier.get();
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
