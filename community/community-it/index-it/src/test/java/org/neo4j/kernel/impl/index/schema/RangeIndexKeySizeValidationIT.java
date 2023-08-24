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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPageCache.config;
import static org.neo4j.kernel.impl.index.schema.PointKeyUtil.SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_BOOLEAN;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_DATE;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_DURATION;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_LOCAL_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_LOCAL_TIME;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_BYTE;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_DOUBLE;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_FLOAT;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_INT;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_LONG;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_NUMBER_SHORT;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_STRING_LENGTH;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_ZONED_DATE_TIME;
import static org.neo4j.kernel.impl.index.schema.Types.SIZE_ZONED_TIME;
import static org.neo4j.test.TestLabels.LABEL_ONE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.index.internal.gbptree.DynamicSizeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.tags.MultiVersionedTag;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
@ExtendWith(RandomExtension.class)
public class RangeIndexKeySizeValidationIT {
    private static final String[] PROP_KEYS = new String[] {"prop0", "prop1", "prop2", "prop3", "prop4"};
    private static final int PAGE_SIZE_8K = (int) ByteUnit.kibiBytes(8);
    private static final int PAGE_SIZE_16K = (int) ByteUnit.kibiBytes(16);
    private static final int ESTIMATED_OVERHEAD_PER_SLOT = 2;
    private static final int WIGGLE_ROOM = 50;

    @Inject
    private DefaultFileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private RandomSupport random;

    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;
    private JobScheduler scheduler;
    private PageCache pageCache;

    @AfterEach
    void cleanup() throws Exception {
        if (dbms != null) {
            dbms.shutdown();
            dbms = null;
            db = null;
        }
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = null;
        }
    }

    /**
     * Key size validation test for single type.
     *
     * Validate that we handle index reads and writes correctly for dynamically sized values (arrays and strings)
     * of all different types with length close to and over the max limit for given type.
     *
     * We do this by inserting arrays of increasing size (doubling each iteration) and when we hit the upper limit
     * we do binary search between the established min and max limit.
     * We also verify that the largest successful array length for each type is as expected because this value
     * is documented and if it changes, documentation also needs to change.
     */
    @ParameterizedTest
    @MethodSource("payloadSize")
    @MultiVersionedTag
    void shouldEnforceSizeCapSingleValueSingleType(int pageSize) {
        startDb(pageSize);
        List<String> failureMessages = new ArrayList<>();
        NamedDynamicValueGenerator[] dynamicValueGenerators = NamedDynamicValueGenerator.values();
        for (NamedDynamicValueGenerator generator : dynamicValueGenerators) {
            int expectedMax = generator.expectedMax;
            String propKey = PROP_KEYS[0] + generator.name();
            createIndex(propKey);

            BinarySearch binarySearch = new BinarySearch();
            Object propValue;

            while (!binarySearch.finished()) {
                propValue = generator.dynamicValue(random, binarySearch.arrayLength);
                long expectedNodeId = -1;

                // Write
                boolean wasAbleToWrite = true;
                try (Transaction tx = db.beginTx()) {
                    Node node = tx.createNode(LABEL_ONE);
                    node.setProperty(propKey, propValue);
                    expectedNodeId = node.getId();
                    tx.commit();
                } catch (Exception e) {
                    wasAbleToWrite = false;
                }

                // Read
                verifyReadExpected(propKey, propValue, expectedNodeId, wasAbleToWrite);

                // Progress binary search
                binarySearch.progress(wasAbleToWrite);
            }
            if (expectedMax != binarySearch.longestSuccessful) {
                failureMessages.add(
                        generator.name() + ": expected=" + expectedMax + ", actual=" + binarySearch.longestSuccessful);
            }
        }
        if (failureMessages.size() > 0) {
            StringJoiner joiner = new StringJoiner(
                    System.lineSeparator(),
                    "Some value types did not have expected longest successful array. "
                            + "This is a strong indicator that documentation of max limit needs to be updated."
                            + System.lineSeparator(),
                    "");
            for (String failureMessage : failureMessages) {
                joiner.add(failureMessage);
            }
            fail(joiner.toString());
        }
    }

    private static class BinarySearch {
        private int longestSuccessful;
        private int minArrayLength;
        private int maxArrayLength = 1;
        private int arrayLength = 1;
        private boolean foundMaxLimit;

        boolean finished() {
            // When arrayLength is stable on minArrayLength, our binary search for max limit is finished
            return arrayLength == minArrayLength;
        }

        void progress(boolean wasAbleToWrite) {
            if (wasAbleToWrite) {
                longestSuccessful = Math.max(arrayLength, longestSuccessful);
                if (!foundMaxLimit) {
                    // We continue to double the max limit until we find some upper limit
                    minArrayLength = arrayLength;
                    maxArrayLength *= 2;
                    arrayLength = maxArrayLength;
                } else {
                    // We where able to write so we can move min limit up to current array length
                    minArrayLength = arrayLength;
                    arrayLength = (minArrayLength + maxArrayLength) / 2;
                }
            } else {
                foundMaxLimit = true;
                // We where not able to write so we take max limit down to current array length
                maxArrayLength = arrayLength;
                arrayLength = (minArrayLength + maxArrayLength) / 2;
            }
        }
    }

    /**
     * Key size validation test for mixed types in composite index.
     *
     * Validate that we handle index reads and writes correctly for
     * dynamically sized values (arrays and strings) of all different
     * types with length close to and over the max limit for given
     * type.
     *
     * We do this by trying to insert random dynamically sized values
     * with size in range that covers the limit, taking into account
     * the number of slots in the index.
     * Then we verify that we either
     *  - write successfully and are able to read value back
     *  - fail to write and no result is found during read
     *
     * Even though we don't keep track of all inserted values, the
     * probability that we will ever generate two identical values
     * is, for single property boolean array which is the most likely,
     * (1/2)^3995. As a reference (1/2)^100 = 7.8886091e-31.
     */
    @ParameterizedTest
    @MethodSource("payloadSize")
    void shouldEnforceSizeCapMixedTypes(int pageSize) {
        startDb(pageSize);
        for (int numberOfSlots = 1; numberOfSlots < 5; numberOfSlots++) {
            String[] propKeys = generatePropertyKeys(numberOfSlots);

            createIndex(propKeys);
            int keySizeLimit = DynamicSizeUtil.keyValueSizeCapFromPageSize(pageSize - calculateReservedBytes());
            int keySizeLimitPerSlot = keySizeLimit / propKeys.length - ESTIMATED_OVERHEAD_PER_SLOT;
            int wiggleRoomPerSlot = WIGGLE_ROOM / propKeys.length;
            SuccessAndFail successAndFail = new SuccessAndFail();
            for (int i = 0; i < 1_000; i++) {
                Object[] propValues = generatePropertyValues(propKeys, keySizeLimitPerSlot, wiggleRoomPerSlot);
                long expectedNodeId = -1;

                // Write
                boolean ableToWrite = true;
                try (Transaction tx = db.beginTx()) {
                    Node node = tx.createNode(LABEL_ONE);
                    setProperties(propKeys, propValues, node);
                    expectedNodeId = node.getId();
                    tx.commit();
                } catch (Exception e) {
                    ableToWrite = false;
                }
                successAndFail.ableToWrite(ableToWrite);

                // Read
                verifyReadExpected(propKeys, propValues, expectedNodeId, ableToWrite);
            }
            successAndFail.verifyBothSuccessAndFail();
        }
    }

    private int calculateReservedBytes() {
        return pageCache.pageReservedBytes(db.getDependencyResolver()
                .resolveDependency(StorageEngine.class)
                .getOpenOptions());
    }

    private static Stream<Integer> payloadSize() {
        return Stream.of(PAGE_SIZE_8K, PAGE_SIZE_16K);
    }

    private static void setProperties(String[] propKeys, Object[] propValues, Node node) {
        for (int propKey = 0; propKey < propKeys.length; propKey++) {
            node.setProperty(propKeys[propKey], propValues[propKey]);
        }
    }

    private static String[] generatePropertyKeys(int numberOfSlots) {
        String[] propKeys = new String[numberOfSlots];
        for (int i = 0; i < numberOfSlots; i++) {
            // Use different property keys for each iteration
            propKeys[i] = PROP_KEYS[i] + "numberOfSlots" + numberOfSlots;
        }
        return propKeys;
    }

    private Object[] generatePropertyValues(String[] propKeys, int keySizeLimitPerSlot, int wiggleRoomPerSlot) {
        Object[] propValues = new Object[propKeys.length];
        for (int propKey = 0; propKey < propKeys.length; propKey++) {
            NamedDynamicValueGenerator among = random.among(NamedDynamicValueGenerator.values());
            propValues[propKey] = among.dynamicValue(random, keySizeLimitPerSlot, wiggleRoomPerSlot);
        }
        return propValues;
    }

    private void verifyReadExpected(String propKey, Object propValue, long expectedNodeId, boolean ableToWrite) {
        verifyReadExpected(new String[] {propKey}, new Object[] {propValue}, expectedNodeId, ableToWrite);
    }

    private void verifyReadExpected(String[] propKeys, Object[] propValues, long expectedNodeId, boolean ableToWrite) {
        try (Transaction tx = db.beginTx()) {
            Map<String, Object> values = new HashMap<>();
            for (int propKey = 0; propKey < propKeys.length; propKey++) {
                values.put(propKeys[propKey], propValues[propKey]);
            }
            try (var nodes = tx.findNodes(LABEL_ONE, values)) {
                if (ableToWrite) {
                    assertTrue(nodes.hasNext());
                    Node node = nodes.next();
                    assertNotNull(node);
                    assertEquals(expectedNodeId, node.getId(), "node id");
                } else {
                    assertFalse(nodes.hasNext());
                }
            }
            tx.commit();
        }
    }

    private void createIndex(String... propKeys) {
        try (Transaction tx = db.beginTx()) {
            IndexCreator indexCreator = tx.schema().indexFor(LABEL_ONE).withIndexType(IndexType.RANGE);
            for (String propKey : propKeys) {
                indexCreator = indexCreator.on(propKey);
            }
            indexCreator.create();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }
    }

    private void startDb(int pageSize) {
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder(neo4jLayout);
        scheduler = JobSchedulerFactory.createInitialisedScheduler();
        pageCache = StandalonePageCacheFactory.createPageCache(
                fs, scheduler, PageCacheTracer.NULL, config(100).pageSize(pageSize));
        builder.setExternalDependencies(dependenciesOf(pageCache));

        dbms = builder.build();
        db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
    }

    private static class SuccessAndFail {
        boolean atLeastOneSuccess;
        boolean atLeastOneFail;

        void ableToWrite(boolean ableToWrite) {
            if (ableToWrite) {
                atLeastOneSuccess = true;
            } else {
                atLeastOneFail = true;
            }
        }

        void verifyBothSuccessAndFail() {
            assertTrue(atLeastOneSuccess, "not a single successful write, need to adjust parameters");
            assertTrue(atLeastOneFail, "not a single failed write, need to adjust parameters");
        }
    }

    private enum NamedDynamicValueGenerator {
        string(Byte.BYTES, 8164, (random, i) -> random.randomValues()
                .nextAlphaNumericTextValue(i, i)
                .stringValue()),
        byteArray(SIZE_NUMBER_BYTE, 8163, (random, i) -> random.randomValues().nextByteArrayRaw(i, i)),
        shortArray(SIZE_NUMBER_SHORT, 4081, (random, i) -> random.randomValues().nextShortArrayRaw(i, i)),
        intArray(SIZE_NUMBER_INT, 2040, (random, i) -> random.randomValues().nextIntArrayRaw(i, i)),
        longArray(SIZE_NUMBER_LONG, 1020, (random, i) -> random.randomValues().nextLongArrayRaw(i, i)),
        floatArray(SIZE_NUMBER_FLOAT, 2040, (random, i) -> random.randomValues().nextFloatArrayRaw(i, i)),
        doubleArray(
                SIZE_NUMBER_DOUBLE, 1020, (random, i) -> random.randomValues().nextDoubleArrayRaw(i, i)),
        booleanArray(SIZE_BOOLEAN, 8164, (random, i) -> random.randomValues().nextBooleanArrayRaw(i, i)),
        charArray(Byte.BYTES, 2721, (random, i) -> random.randomValues()
                .nextAlphaNumericTextValue(i, i)
                .stringValue()
                .toCharArray()),
        stringArray1(SIZE_STRING_LENGTH + 1, 2721, (random, i) -> random.randomValues()
                .nextAlphaNumericStringArrayRaw(i, i, 1, 1)),
        stringArray10(SIZE_STRING_LENGTH + 10, 680, (random, i) -> random.randomValues()
                .nextAlphaNumericStringArrayRaw(i, i, 10, 10)),
        stringArray100(SIZE_STRING_LENGTH + 100, 80, (random, i) -> random.randomValues()
                .nextAlphaNumericStringArrayRaw(i, i, 100, 100)),
        stringArray1000(SIZE_STRING_LENGTH + 1000, 8, (random, i) -> random.randomValues()
                .nextAlphaNumericStringArrayRaw(i, i, 1000, 1000)),
        dateArray(SIZE_DATE, 1020, (random, i) -> random.randomValues().nextDateArrayRaw(i, i)),
        timeArray(SIZE_ZONED_TIME, 680, (random, i) -> random.randomValues().nextTimeArrayRaw(i, i)),
        localTimeArray(
                SIZE_LOCAL_TIME, 1020, (random, i) -> random.randomValues().nextLocalTimeArrayRaw(i, i)),
        dateTimeArray(
                SIZE_ZONED_DATE_TIME, 510, (random, i) -> random.randomValues().nextDateTimeArrayRaw(i, i)),
        localDateTimeArray(
                SIZE_LOCAL_DATE_TIME, 680, (random, i) -> random.randomValues().nextLocalDateTimeArrayRaw(i, i)),
        durationArray(SIZE_DURATION, 291, (random, i) -> random.randomValues().nextDurationArrayRaw(i, i)),
        periodArray(SIZE_DURATION, 291, (random, i) -> random.randomValues().nextPeriodArrayRaw(i, i)),
        cartesianPointArray(SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE, 510, (random, i) -> random.randomValues()
                .nextCartesianPointArray(i, i)
                .asObjectCopy()),
        cartesian3DPointArray(SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE, 340, (random, i) -> random.randomValues()
                .nextCartesian3DPointArray(i, i)
                .asObjectCopy()),
        geographicPointArray(SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE, 510, (random, i) -> random.randomValues()
                .nextGeographicPointArray(i, i)
                .asObjectCopy()),
        geographic3DPointArray(
                SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE, 340, (random, i) -> random.randomValues()
                        .nextGeographic3DPointArray(i, i)
                        .asObjectCopy());

        private final int singleArrayEntrySize;
        private final DynamicValueGenerator generator;
        private final int expectedMax;

        NamedDynamicValueGenerator(
                int singleArrayEntrySize, int expectedLongestArrayLength, DynamicValueGenerator generator) {
            this.singleArrayEntrySize = singleArrayEntrySize;
            this.expectedMax = expectedLongestArrayLength;
            this.generator = generator;
        }

        Object dynamicValue(RandomSupport random, int length) {
            return generator.dynamicValue(random, length);
        }

        Object dynamicValue(RandomSupport random, int keySizeLimit, int wiggleRoom) {
            int lowLimit = lowLimit(keySizeLimit, wiggleRoom, singleArrayEntrySize);
            int highLimit = highLimit(keySizeLimit, wiggleRoom, singleArrayEntrySize);
            return dynamicValue(random, random.intBetween(lowLimit, highLimit));
        }

        private static int lowLimit(int keySizeLimit, int wiggleRoom, int singleEntrySize) {
            return (keySizeLimit - wiggleRoom) / singleEntrySize;
        }

        private static int highLimit(int keySizeLimit, int wiggleRoom, int singleEntrySize) {
            return (keySizeLimit + wiggleRoom) / singleEntrySize;
        }

        @FunctionalInterface
        private interface DynamicValueGenerator {
            Object dynamicValue(RandomSupport random, int arrayLength);
        }
    }
}
