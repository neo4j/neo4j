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
package org.neo4j.kernel.impl.store;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.schema.SchemaRuleMapifier.PROP_OWNING_CONSTRAINT;
import static org.neo4j.internal.schema.SchemaRuleMapifier.mapifySchemaRule;
import static org.neo4j.internal.schema.SchemaRuleMapifier.unmapifySchemaRule;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Value;

/**
 * In this schema store implementation, each schema record is really just a pointer to a property chain in the property store.
 * The properties describe each schema rule structurally, as a map of property keys to values. The property keys are resolved as property key tokens
 * with pre-defined names. The property keys can vary from database to database, but the token names are the same.
 * <p>
 * The exact structure of a schema rule depends on what kind of rule it is:
 *
 * <ul>
 *     <li>All</li>
 *     <ul>
 *         <li>schemaRuleType: String, "INDEX" or "CONSTRAINT"</li>
 *         <li>name: String</li>
 *         <li>Schema descriptor:</li>
 *         <ul>
 *             <li>schemaEntityType: String, "NODE" or "RELATIONSHIP"</li>
 *             <li>schemaSchemaPatternMatchingType: String, "COMPLETE_ALL_TOKENS" or "PARTIAL_ANY_TOKEN"</li>
 *             <li>schemaEntityIds: int[] -- IDs for either labels or relationship types, depending on schemaEntityType</li>
 *             <li>schemaPropertyIds: int[]</li>
 *         </ul>
 *     </ul>
 *     <li>INDEXes</li>
 *     <ul>
 *         <li>schemaRuleType = "INDEX"</li>
 *         <li>indexRuleType: String, "UNIQUE" or "NON_UNIQUE"</li>
 *         <li>owningConstraint: long -- only present for indexRuleType=UNIQUE indexes</li>
 *         <li>indexProviderName: String</li>
 *         <li>indexProviderVersion: String</li>
 *         <li>"indexConfig.XYZ"... properties -- index specific settings, depending on the index provider</li>
 *     </ul>
 *     <li>CONSTRAINTs</li>
 *     <ul>
 *         <li>schemaRuleType = "CONSTRAINT"</li>
 *         <li>constraintRuleType: String, "UNIQUE" or "EXISTS" or "UNIQUE_EXISTS"</li>
 *         <li>ownedIndex: long -- only present for constraintRuleType=UNIQUE or constraintRuleType=UNIQUE_EXISTS constraints</li>
 *     </ul>
 * </ul>
 */
public class SchemaStore extends CommonAbstractStore<SchemaRecord, IntStoreHeader> {
    // We technically don't need a store header, but we reserve record id 0 anyway, both to stay compatible with the old
    // schema store,
    // and to have it in reserve, just in case we might need it in the future.
    private static final IntStoreHeaderFormat VALID_STORE_HEADER = new IntStoreHeaderFormat(0);

    public static final String TYPE_DESCRIPTOR = "SchemaStore";
    private final PropertyStore propertyStore;

    public SchemaStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            PropertyStore propertyStore,
            RecordFormats recordFormats,
            boolean readOnly,
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
                getStoreHeaderFormat(),
                readOnly,
                databaseName,
                openOptions);
        this.propertyStore = propertyStore;
    }

    private static IntStoreHeaderFormat getStoreHeaderFormat() {
        return VALID_STORE_HEADER;
    }

    public PropertyStore propertyStore() {
        return propertyStore;
    }

    public static int getOwningConstraintPropertyKeyId(TokenHolders tokenHolders) throws KernelException {
        int[] ids = new int[1];
        tokenHolders.propertyKeyTokens().getOrCreateInternalIds(new String[] {PROP_OWNING_CONSTRAINT}, ids);
        return ids[0];
    }

    public static SchemaRule readSchemaRule(
            SchemaRecord record,
            PropertyStore propertyStore,
            TokenHolders tokenHolders,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker)
            throws MalformedSchemaRuleException {
        Map<String, Value> map = schemaRecordToMap(record, propertyStore, tokenHolders, storeCursors, memoryTracker);
        return unmapifySchemaRule(record.getId(), map);
    }

    private static Map<String, Value> schemaRecordToMap(
            SchemaRecord record,
            PropertyStore propertyStore,
            TokenHolders tokenHolders,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker)
            throws MalformedSchemaRuleException {
        Map<String, Value> props = new HashMap<>();
        PropertyRecord propRecord = propertyStore.newRecord();
        long nextProp = record.getNextProp();
        while (nextProp != NO_NEXT_PROPERTY.longValue()) {
            try {
                propertyStore.getRecordByCursor(
                        nextProp,
                        propRecord,
                        RecordLoad.NORMAL,
                        storeCursors.readCursor(PROPERTY_CURSOR),
                        memoryTracker);
            } catch (InvalidRecordException e) {
                throw new MalformedSchemaRuleException(
                        "Cannot read schema rule because it is referencing a property record (id " + nextProp
                                + ") that is invalid: " + propRecord,
                        e);
            }
            for (PropertyBlock propertyBlock : propRecord) {
                PropertyKeyValue propertyKeyValue =
                        propertyBlock.newPropertyKeyValue(propertyStore, storeCursors, memoryTracker);
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

    public static IntObjectMap<Value> convertSchemaRuleToMap(SchemaRule rule, TokenHolders tokenHolders)
            throws KernelException {
        // The dance we do in here with map to arrays to another map, allows us to resolve (and allocate) all of the
        // tokens in a single batch operation.
        Map<String, Value> stringlyMap = mapifySchemaRule(rule);

        int size = stringlyMap.size();
        String[] keys = new String[size];
        int[] keyIds = new int[size];
        Value[] values = new Value[size];

        Iterator<Map.Entry<String, Value>> itr = stringlyMap.entrySet().iterator();
        for (int i = 0; i < size; i++) {
            Map.Entry<String, Value> entry = itr.next();
            keys[i] = entry.getKey();
            values[i] = entry.getValue();
        }

        tokenHolders.propertyKeyTokens().getOrCreateInternalIds(keys, keyIds);

        MutableIntObjectMap<Value> tokenisedMap = new IntObjectHashMap<>();
        for (int i = 0; i < size; i++) {
            tokenisedMap.put(keyIds[i], values[i]);
        }

        return tokenisedMap;
    }
}
