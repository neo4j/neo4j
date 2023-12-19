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
package org.neo4j.kernel.impl.storemigration.legacy;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.PropertySchemaType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorImplementation;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.IntStoreHeaderFormat;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.storemigration.SchemaStore44MigrationUtil;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

public class SchemaStore44Reader implements AutoCloseable {

    private static final String PROP_SCHEMA_RULE_PREFIX = "__org.neo4j.SchemaRule.";
    private static final String PROP_SCHEMA_RULE_TYPE =
            PROP_SCHEMA_RULE_PREFIX + "schemaRuleType"; // index / constraint
    private static final String PROP_INDEX_RULE_TYPE = PROP_SCHEMA_RULE_PREFIX + "indexRuleType"; // Uniqueness
    private static final String PROP_CONSTRAINT_RULE_TYPE =
            PROP_SCHEMA_RULE_PREFIX + "constraintRuleType"; // Existence / Uniqueness / ...
    private static final String PROP_SCHEMA_RULE_NAME = PROP_SCHEMA_RULE_PREFIX + "name";
    private static final String PROP_OWNED_INDEX = PROP_SCHEMA_RULE_PREFIX + "ownedIndex";
    private static final String PROP_OWNING_CONSTRAINT = PROP_SCHEMA_RULE_PREFIX + "owningConstraint";
    private static final String PROP_INDEX_PROVIDER_NAME = PROP_SCHEMA_RULE_PREFIX + "indexProviderName";
    private static final String PROP_INDEX_PROVIDER_VERSION = PROP_SCHEMA_RULE_PREFIX + "indexProviderVersion";
    private static final String PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE = PROP_SCHEMA_RULE_PREFIX + "schemaEntityType";
    private static final String PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS = PROP_SCHEMA_RULE_PREFIX + "schemaEntityIds";
    private static final String PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS = PROP_SCHEMA_RULE_PREFIX + "schemaPropertyIds";
    private static final String PROP_SCHEMA_DESCRIPTOR_PROPERTY_SCHEMA_TYPE =
            PROP_SCHEMA_RULE_PREFIX + "schemaPropertySchemaType";

    private static final String PROP_INDEX_TYPE = PROP_SCHEMA_RULE_PREFIX + "indexType";
    private static final String PROP_INDEX_CONFIG_PREFIX = PROP_SCHEMA_RULE_PREFIX + "IndexConfig.";

    private static final Function<Long, SchemaRule44.Index> FORMER_LABEL_SCAN_STORE_SCHEMA_RULE_FACTORY =
            id -> new SchemaRule44.Index(
                    id,
                    SchemaStore44MigrationUtil.FORMER_LABEL_SCAN_STORE_SCHEMA,
                    false,
                    SchemaStore44MigrationUtil.FORMER_LABEL_SCAN_STORE_GENERATED_NAME,
                    SchemaRule44.IndexType.LOOKUP,
                    new IndexProviderDescriptor("token-lookup", "1.0"),
                    IndexConfig.empty(),
                    null);

    private final SchemaStore44 schemaStore;
    private final PropertyStore propertyStore;
    private final TokenHolders tokenHolders;
    private final KernelVersion kernelVersion;

    public SchemaStore44Reader(
            FileSystemAbstraction fileSystem,
            PropertyStore propertyStore,
            TokenHolders tokenHolders,
            KernelVersion kernelVersion,
            Path schemaStoreLocation,
            Path idFile,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory cursorContextFactory,
            InternalLogProvider logProvider,
            RecordFormats recordFormats,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        this.propertyStore = propertyStore;
        this.tokenHolders = tokenHolders;
        this.kernelVersion = kernelVersion;
        this.schemaStore = new SchemaStore44(
                fileSystem,
                schemaStoreLocation,
                idFile,
                conf,
                idType,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                cursorContextFactory,
                logProvider,
                recordFormats,
                databaseName,
                openOptions);
    }

    public List<SchemaRule44> loadAllSchemaRules(StoreCursors storeCursors) {
        long startId = schemaStore.getNumberOfReservedLowIds();
        long endId = schemaStore.getIdGenerator().getHighId();

        List<SchemaRule44> schemaRules = new ArrayList<>();
        maybeAddFormerLabelScanStore(schemaRules);
        for (long id = startId; id < endId; id++) {
            SchemaRecord schemaRecord = schemaStore.getRecordByCursor(
                    id, schemaStore.newRecord(), RecordLoad.LENIENT_ALWAYS, storeCursors.readCursor(SCHEMA_CURSOR));
            if (!schemaRecord.inUse()) {
                continue;
            }

            try {
                Map<String, Value> propertyKeyValue = schemaRecordToMap(schemaRecord, storeCursors);
                SchemaRule44 schemaRule = createSchemaRule(id, propertyKeyValue);
                schemaRules.add(schemaRule);
            } catch (MalformedSchemaRuleException ignored) {

            }
        }

        return schemaRules;
    }

    private void maybeAddFormerLabelScanStore(List<SchemaRule44> schemaRules) {
        if (kernelVersion.isLessThan(KernelVersion.VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED)) {
            schemaRules.add(constructFormerLabelScanStoreSchemaRule());
        }
    }

    private Map<String, Value> schemaRecordToMap(SchemaRecord record, StoreCursors storeCursors)
            throws MalformedSchemaRuleException {
        Map<String, Value> props = new HashMap<>();
        PropertyRecord propRecord = propertyStore.newRecord();
        long nextProp = record.getNextProp();
        while (nextProp != NO_NEXT_PROPERTY.longValue()) {
            try {
                propertyStore.getRecordByCursor(
                        nextProp, propRecord, RecordLoad.NORMAL, storeCursors.readCursor(PROPERTY_CURSOR));
            } catch (InvalidRecordException e) {
                throw new MalformedSchemaRuleException(
                        "Cannot read schema rule because it is referencing a property record (id " + nextProp
                                + ") that is invalid: " + propRecord,
                        e);
            }
            for (PropertyBlock propertyBlock : propRecord) {
                PropertyKeyValue propertyKeyValue = propertyBlock.newPropertyKeyValue(propertyStore, storeCursors);
                insertPropertyIntoMap(propertyKeyValue, props, tokenHolders);
            }
            nextProp = propRecord.getNextProp();
        }
        return props;
    }

    private static void insertPropertyIntoMap(
            PropertyKeyValue propertyKeyValue, Map<String, Value> props, TokenHolders tokenHolders)
            throws MalformedSchemaRuleException {
        try {
            NamedToken propertyKeyTokenName =
                    tokenHolders.propertyKeyTokens().getInternalTokenById(propertyKeyValue.propertyKeyId());
            props.put(propertyKeyTokenName.name(), propertyKeyValue.value());
        } catch (TokenNotFoundException | InvalidRecordException e) {
            int id = propertyKeyValue.propertyKeyId();
            throw new MalformedSchemaRuleException(
                    "Cannot read schema rule because it is referring to a property key token (id " + id
                            + ") that does not exist.",
                    e);
        }
    }

    private SchemaRule44 createSchemaRule(long ruleId, Map<String, Value> props) throws MalformedSchemaRuleException {
        if (props.isEmpty()) {
            return constructFormerLabelScanStoreSchemaRule(ruleId);
        }

        String schemaRuleType = getString(PROP_SCHEMA_RULE_TYPE, props);
        return switch (schemaRuleType) {
            case "INDEX" -> buildIndexRule(ruleId, props);
            case "CONSTRAINT" -> buildConstraintRule(ruleId, props);
            default -> throw new MalformedSchemaRuleException(
                    "Can not create a schema rule of type: " + schemaRuleType);
        };
    }

    public static SchemaRule44 constructFormerLabelScanStoreSchemaRule() {
        return constructFormerLabelScanStoreSchemaRule(IndexDescriptor.FORMER_LABEL_SCAN_STORE_ID);
    }

    /**
     * HISTORICAL NOTE:
     * <p>
     * Before 4.3, there was an index-like structure called Label scan store
     * and it was turned into a proper index in 4.3. However, because Label scan store
     * did not start its life as an index it is a bit special and there is some
     * historical baggage attached to it.
     * The schema store record describing former Label scan store was written to schema store,
     * when kernel version was changed. Also because technical limitations at the time,
     * former Label scan store is represented by a special record without any properties.
     * As a result there are two special cases associated with former Label scan store:
     * <ul>
     *     <li>
     *     If the kernel version is less than 4.3, it means that the former Label scan store
     *     exists despite not having a corresponding record in schema store.
     *     It is certain that it exists in such case, because it could not have been dropped
     *     before the kernel version upgrade to 4.3 which is the version where its full index-like
     *     capabilities (including the possibility of being dropped) were unlocked.
     *     </li>
     *     <li>
     *     There can be a property-less schema record in schema store. Such record must
     *     be interpreted as the former Label scan store.
     *     </li>
     * </ul>
     */
    public static SchemaRule44 constructFormerLabelScanStoreSchemaRule(long ruleId) {
        return FORMER_LABEL_SCAN_STORE_SCHEMA_RULE_FACTORY.apply(ruleId);
    }

    private static SchemaRule44.Index buildIndexRule(long schemaRuleId, Map<String, Value> props)
            throws MalformedSchemaRuleException {
        SchemaDescriptor schema = buildSchemaDescriptor(props);
        boolean unique = parseIndexRuleType(getString(PROP_INDEX_RULE_TYPE, props));
        String name = getString(PROP_SCHEMA_RULE_NAME, props);
        SchemaRule44.IndexType indexType = getIndexType(getString(PROP_INDEX_TYPE, props));
        IndexConfig indexConfig = extractIndexConfig(props);

        String providerKey = getString(PROP_INDEX_PROVIDER_NAME, props);
        String providerVersion = getString(PROP_INDEX_PROVIDER_VERSION, props);
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor(providerKey, providerVersion);

        Long owningConstraintId = null;
        if (props.containsKey(PROP_OWNING_CONSTRAINT)) {
            owningConstraintId = getLong(PROP_OWNING_CONSTRAINT, props);
        }

        return new SchemaRule44.Index(
                schemaRuleId, schema, unique, name, indexType, providerDescriptor, indexConfig, owningConstraintId);
    }

    private static SchemaRule44.Constraint buildConstraintRule(long id, Map<String, Value> props)
            throws MalformedSchemaRuleException {
        SchemaDescriptor schema = buildSchemaDescriptor(props);
        SchemaRule44.ConstraintRuleType constraintRuleType =
                getConstraintRuleType(getString(PROP_CONSTRAINT_RULE_TYPE, props));
        String name = getString(PROP_SCHEMA_RULE_NAME, props);
        Long ownedIndex = getOptionalLong(PROP_OWNED_INDEX, props);
        SchemaRule44.IndexType indexType = getIndexType(getOptionalString(PROP_INDEX_TYPE, props));
        return new SchemaRule44.Constraint(id, schema, name, constraintRuleType, ownedIndex, indexType);
    }

    private static boolean parseIndexRuleType(String indexRuleType) throws MalformedSchemaRuleException {
        return switch (indexRuleType) {
            case "NON_UNIQUE" -> false;
            case "UNIQUE" -> true;
            default -> throw new MalformedSchemaRuleException("Did not recognize index rule type: " + indexRuleType);
        };
    }

    private static SchemaDescriptor buildSchemaDescriptor(Map<String, Value> props)
            throws MalformedSchemaRuleException {
        EntityType entityType = getEntityType(getString(PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE, props));
        PropertySchemaType propertySchemaType =
                getPropertySchemaType(getString(PROP_SCHEMA_DESCRIPTOR_PROPERTY_SCHEMA_TYPE, props));
        int[] entityIds = getIntArray(PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS, props);
        int[] propertyIds = getIntArray(PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS, props);

        return new SchemaDescriptorImplementation(entityType, propertySchemaType, entityIds, propertyIds);
    }

    private static IndexConfig extractIndexConfig(Map<String, Value> props) {
        Map<String, Value> configMap = new HashMap<>();
        for (Map.Entry<String, Value> entry : props.entrySet()) {
            if (entry.getKey().startsWith(PROP_INDEX_CONFIG_PREFIX)) {
                configMap.put(entry.getKey().substring(PROP_INDEX_CONFIG_PREFIX.length()), entry.getValue());
            }
        }
        return IndexConfig.with(configMap);
    }

    private static SchemaRule44.IndexType getIndexType(String indexType) throws MalformedSchemaRuleException {
        if (indexType == null) {
            return null;
        }

        try {
            return SchemaRule44.IndexType.valueOf(indexType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException("Did not recognize index type: " + indexType, e);
        }
    }

    private static SchemaRule44.ConstraintRuleType getConstraintRuleType(String constraintRuleType)
            throws MalformedSchemaRuleException {
        try {
            return SchemaRule44.ConstraintRuleType.valueOf(constraintRuleType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException("Did not recognize constraint rule type: " + constraintRuleType, e);
        }
    }

    private static PropertySchemaType getPropertySchemaType(String propertySchemaType)
            throws MalformedSchemaRuleException {
        try {
            return PropertySchemaType.valueOf(propertySchemaType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException("Did not recognize property schema type: " + propertySchemaType, e);
        }
    }

    private static EntityType getEntityType(String entityType) throws MalformedSchemaRuleException {
        try {
            return EntityType.valueOf(entityType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException("Did not recognize entity type: " + entityType, e);
        }
    }

    private static int[] getIntArray(String property, Map<String, Value> props) throws MalformedSchemaRuleException {
        Value value = props.get(property);
        if (value instanceof IntArray) {
            return (int[]) value.asObject();
        }
        throw new MalformedSchemaRuleException("Expected property " + property + " to be a IntArray but was " + value);
    }

    private static long getLong(String property, Map<String, Value> props) throws MalformedSchemaRuleException {
        Value value = props.get(property);
        if (value instanceof LongValue) {
            return ((LongValue) value).value();
        }
        throw new MalformedSchemaRuleException("Expected property " + property + " to be a LongValue but was " + value);
    }

    private static Long getOptionalLong(String property, Map<String, Value> props) {
        Value value = props.get(property);
        if (value instanceof LongValue) {
            return ((LongValue) value).value();
        }
        return null;
    }

    private static String getString(String property, Map<String, Value> map) throws MalformedSchemaRuleException {
        Value value = map.get(property);
        if (value instanceof TextValue) {
            return ((TextValue) value).stringValue();
        }
        throw new MalformedSchemaRuleException("Expected property " + property + " to be a TextValue but was " + value);
    }

    private static String getOptionalString(String property, Map<String, Value> map) {
        Value value = map.get(property);
        if (value instanceof TextValue) {
            return ((TextValue) value).stringValue();
        }
        return null;
    }

    @Override
    public void close() {
        schemaStore.close();
    }

    // this is using the same SchemaRecord as the 5.0+ Schema store because it has not changed at all.
    private static class SchemaStore44 extends CommonAbstractStore<SchemaRecord, IntStoreHeader> {
        private static final IntStoreHeaderFormat VALID_STORE_HEADER = new IntStoreHeaderFormat(0);

        private static final String TYPE_DESCRIPTOR = "SchemaStore44";

        SchemaStore44(
                FileSystemAbstraction fileSystem,
                Path path,
                Path idFile,
                Config conf,
                IdType idType,
                IdGeneratorFactory idGeneratorFactory,
                PageCache pageCache,
                PageCacheTracer pageCacheTracer,
                CursorContextFactory cursorContextFactory,
                InternalLogProvider logProvider,
                RecordFormats recordFormats,
                String databaseName,
                ImmutableSet<OpenOption> openOptions) {
            super(
                    fileSystem,
                    path,
                    idFile,
                    conf,
                    idType,
                    idGeneratorFactory,
                    pageCache,
                    pageCacheTracer,
                    logProvider,
                    TYPE_DESCRIPTOR,
                    recordFormats.schema(),
                    VALID_STORE_HEADER,
                    true,
                    databaseName,
                    openOptions);
            initialise(cursorContextFactory);
        }
    }
}
