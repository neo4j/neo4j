/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.PropertySchemaType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorImplementation;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.RecordStorageCapability;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;

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
 *             <li>schemaPropertySchemaType: String, "COMPLETE_ALL_TOKENS" or "PARTIAL_ANY_TOKEN"</li>
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
public class SchemaStore extends CommonAbstractStore<SchemaRecord,IntStoreHeader>
{
    // We technically don't need a store header, but we reserve record id 0 anyway, both to stay compatible with the old schema store,
    // and to have it in reserve, just in case we might need it in the future.
    private static final IntStoreHeaderFormat VALID_STORE_HEADER = new IntStoreHeaderFormat( 0 );

    // When the store format does not support the flexible schema store feature, then we won't even pretend to have a header,
    // since that can run afoul of the id generators, which will be initialised with a max id of zero.
    private static final IntStoreHeaderFormat DISABLED_STORE_HEADER = new ConstantIntStoreHeaderFormat( 0 );

    public static final String TYPE_DESCRIPTOR = "SchemaStore";
    private final PropertyStore propertyStore;

    public SchemaStore(
            Path path,
            Path idFile,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            PropertyStore propertyStore,
            RecordFormats recordFormats,
            ImmutableSet<OpenOption> openOptions )
    {
        super( path, idFile, conf, idType, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR, recordFormats.schema(),
                getStoreHeaderFormat( recordFormats ), recordFormats.storeVersion(), openOptions );
        this.propertyStore = propertyStore;
    }

    private static IntStoreHeaderFormat getStoreHeaderFormat( RecordFormats recordFormats )
    {
        return recordFormats.hasCapability( RecordStorageCapability.FLEXIBLE_SCHEMA_STORE ) ? VALID_STORE_HEADER : DISABLED_STORE_HEADER;
    }

    public PropertyStore propertyStore()
    {
        return propertyStore;
    }

    private static final String PROP_SCHEMA_RULE_PREFIX = "__org.neo4j.SchemaRule.";
    private static final String PROP_SCHEMA_RULE_TYPE = PROP_SCHEMA_RULE_PREFIX + "schemaRuleType"; // index / constraint
    private static final String PROP_INDEX_RULE_TYPE = PROP_SCHEMA_RULE_PREFIX + "indexRuleType"; // Uniqueness
    private static final String PROP_CONSTRAINT_RULE_TYPE = PROP_SCHEMA_RULE_PREFIX + "constraintRuleType"; // Existence / Uniqueness / ...
    private static final String PROP_SCHEMA_RULE_NAME = PROP_SCHEMA_RULE_PREFIX + "name";
    private static final String PROP_OWNED_INDEX = PROP_SCHEMA_RULE_PREFIX + "ownedIndex";
    private static final String PROP_OWNING_CONSTRAINT = PROP_SCHEMA_RULE_PREFIX + "owningConstraint";
    private static final String PROP_INDEX_PROVIDER_NAME = PROP_SCHEMA_RULE_PREFIX + "indexProviderName";
    private static final String PROP_INDEX_PROVIDER_VERSION = PROP_SCHEMA_RULE_PREFIX + "indexProviderVersion";
    private static final String PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE = PROP_SCHEMA_RULE_PREFIX + "schemaEntityType";
    private static final String PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS = PROP_SCHEMA_RULE_PREFIX + "schemaEntityIds";
    private static final String PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS = PROP_SCHEMA_RULE_PREFIX + "schemaPropertyIds";
    private static final String PROP_SCHEMA_DESCRIPTOR_PROPERTY_SCHEMA_TYPE = PROP_SCHEMA_RULE_PREFIX + "schemaPropertySchemaType";

    private static final String PROP_INDEX_TYPE = PROP_SCHEMA_RULE_PREFIX + "indexType";
    private static final String PROP_INDEX_CONFIG_PREFIX = PROP_SCHEMA_RULE_PREFIX + "IndexConfig.";

    public static int getOwningConstraintPropertyKeyId( TokenHolders tokenHolders ) throws KernelException
    {
        int[] ids = new int[1];
        tokenHolders.propertyKeyTokens().getOrCreateInternalIds( new String[]{PROP_OWNING_CONSTRAINT}, ids );
        return ids[0];
    }

    /**
     * Turn a {@link SchemaRule} into a map-of-string-to-value representation.
     * @param rule the schema rule to convert.
     * @return a map representation of the given schema rule.
     * @see #unmapifySchemaRule(long, Map)
     */
    public static Map<String,Value> mapifySchemaRule( SchemaRule rule )
    {
        Map<String,Value> map = new HashMap<>();
        putStringProperty( map, PROP_SCHEMA_RULE_NAME, rule.getName() );

        // Schema
        schemaDescriptorToMap( rule.schema(), map );

        // Rule
        if ( rule instanceof IndexDescriptor )
        {
            schemaIndexToMap( (IndexDescriptor) rule, map );
        }
        else if ( rule instanceof ConstraintDescriptor )
        {
            schemaConstraintToMap( (ConstraintDescriptor) rule, map );
        }
        return map;
    }

    public static IntObjectMap<Value> convertSchemaRuleToMap( SchemaRule rule, TokenHolders tokenHolders ) throws KernelException
    {
        // The dance we do in here with map to arrays to another map, allows us to resolve (and allocate) all of the tokens in a single batch operation.
        Map<String,Value> stringlyMap = mapifySchemaRule( rule );

        int size = stringlyMap.size();
        String[] keys = new String[size];
        int[] keyIds = new int[size];
        Value[] values = new Value[size];

        Iterator<Map.Entry<String,Value>> itr = stringlyMap.entrySet().iterator();
        for ( int i = 0; i < size; i++ )
        {
            Map.Entry<String,Value> entry = itr.next();
            keys[i] = entry.getKey();
            values[i] = entry.getValue();
        }

        tokenHolders.propertyKeyTokens().getOrCreateInternalIds( keys, keyIds );

        MutableIntObjectMap<Value> tokenisedMap = new IntObjectHashMap<>();
        for ( int i = 0; i < size; i++ )
        {
            tokenisedMap.put( keyIds[i], values[i] );
        }

        return tokenisedMap;
    }

    private static void schemaDescriptorToMap( SchemaDescriptor schemaDescriptor, Map<String,Value> map )
    {
        EntityType entityType = schemaDescriptor.entityType();
        PropertySchemaType propertySchemaType = schemaDescriptor.propertySchemaType();
        int[] entityTokenIds = schemaDescriptor.getEntityTokenIds();
        int[] propertyIds = schemaDescriptor.getPropertyIds();
        putStringProperty( map, PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE, entityType.name() );
        putStringProperty( map, PROP_SCHEMA_DESCRIPTOR_PROPERTY_SCHEMA_TYPE, propertySchemaType.name() );
        putIntArrayProperty( map, PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS, entityTokenIds );
        putIntArrayProperty( map, PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS, propertyIds );
    }

    private static void indexConfigToMap( IndexConfig indexConfig, Map<String,Value> map )
    {
        RichIterable<Pair<String,Value>> entries = indexConfig.entries();
        for ( Pair<String,Value> entry : entries )
        {
            putIndexConfigProperty( map, entry.getOne(), entry.getTwo() );
        }
    }

    private static void schemaIndexToMap( IndexDescriptor rule, Map<String,Value> map )
    {
        // Rule
        putStringProperty( map, PROP_SCHEMA_RULE_TYPE, "INDEX" );

        IndexType indexType = rule.getIndexType();
        putStringProperty( map, PROP_INDEX_TYPE, indexType.name() );

        if ( rule.isUnique() )
        {
            putStringProperty( map, PROP_INDEX_RULE_TYPE, "UNIQUE" );
            if ( rule.getOwningConstraintId().isPresent() )
            {
                map.put( PROP_OWNING_CONSTRAINT, Values.longValue( rule.getOwningConstraintId().getAsLong() ) );
            }
        }
        else
        {
            putStringProperty( map, PROP_INDEX_RULE_TYPE, "NON_UNIQUE" );
        }

        // Provider
        indexProviderToMap( rule, map );

        // Index config
        IndexConfig indexConfig = rule.getIndexConfig();
        indexConfigToMap( indexConfig, map );
    }

    private static void indexProviderToMap( IndexDescriptor rule, Map<String,Value> map )
    {
        IndexProviderDescriptor provider = rule.getIndexProvider();
        String name = provider.getKey();
        String version = provider.getVersion();
        putStringProperty( map, PROP_INDEX_PROVIDER_NAME, name );
        putStringProperty( map, PROP_INDEX_PROVIDER_VERSION, version );
    }

    private static void schemaConstraintToMap( ConstraintDescriptor rule, Map<String,Value> map )
    {
        // Rule
        putStringProperty( map, PROP_SCHEMA_RULE_TYPE, "CONSTRAINT" );
        ConstraintType type = rule.type();
        switch ( type )
        {
        case UNIQUE:
            putStringProperty( map, PROP_CONSTRAINT_RULE_TYPE, "UNIQUE" );
            if ( rule.asIndexBackedConstraint().hasOwnedIndexId() )
            {
                putLongProperty( map, PROP_OWNED_INDEX, rule.asIndexBackedConstraint().ownedIndexId() );
            }
            break;
        case EXISTS:
            putStringProperty( map, PROP_CONSTRAINT_RULE_TYPE, "EXISTS" );
            break;
        case UNIQUE_EXISTS:
            putStringProperty( map, PROP_CONSTRAINT_RULE_TYPE, "UNIQUE_EXISTS" );
            if ( rule.asIndexBackedConstraint().hasOwnedIndexId() )
            {
                putLongProperty( map, PROP_OWNED_INDEX, rule.asIndexBackedConstraint().ownedIndexId() );
            }
            break;
        default:
            throw new UnsupportedOperationException( "Unrecognized constraint type: " + type );
        }
    }

    public static SchemaRule readSchemaRule( SchemaRecord record, PropertyStore propertyStore, TokenHolders tokenHolders, PageCursorTracer cursorTracer )
            throws MalformedSchemaRuleException
    {
        Map<String,Value> map = schemaRecordToMap( record, propertyStore, tokenHolders, cursorTracer );
        return unmapifySchemaRule( record.getId(), map );
    }

    /**
     * Turn a map-of-string-to-value representation of a schema rule, into an actual {@link SchemaRule} object.
     * @param ruleId the id of the rule.
     * @param map the map representation of the schema rule.
     * @return the schema rule object represented by the given map.
     * @throws MalformedSchemaRuleException if the map cannot be cleanly converted to a schema rule.
     * @see #mapifySchemaRule(SchemaRule)
     */
    public static SchemaRule unmapifySchemaRule( long ruleId, Map<String,Value> map ) throws MalformedSchemaRuleException
    {
        String schemaRuleType = getString( PROP_SCHEMA_RULE_TYPE, map );
        switch ( schemaRuleType )
        {
        case "INDEX":
            return buildIndexRule( ruleId, map );
        case "CONSTRAINT":
            return buildConstraintRule( ruleId, map );
        default:
            throw new MalformedSchemaRuleException( "Can not create a schema rule of type: " + schemaRuleType );
        }
    }

    private static SchemaRule buildIndexRule( long schemaRuleId, Map<String,Value> props ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schema = buildSchemaDescriptor( props );
        String indexRuleType = getString( PROP_INDEX_RULE_TYPE, props );
        boolean unique = parseIndexType( indexRuleType );

        IndexPrototype prototype = unique ? IndexPrototype.uniqueForSchema( schema ) : IndexPrototype.forSchema( schema );

        prototype = prototype.withName( getString( PROP_SCHEMA_RULE_NAME, props ) );
        prototype = prototype.withIndexType( getIndexType( getString( PROP_INDEX_TYPE, props ) ) );

        String providerKey = getString( PROP_INDEX_PROVIDER_NAME, props );
        String providerVersion = getString( PROP_INDEX_PROVIDER_VERSION, props );
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( providerKey, providerVersion );
        prototype = prototype.withIndexProvider( providerDescriptor );

        IndexDescriptor index = prototype.materialise( schemaRuleId );

        IndexConfig indexConfig = extractIndexConfig( props );
        index = index.withIndexConfig( indexConfig );

        if ( props.containsKey( PROP_OWNING_CONSTRAINT ) )
        {
            index = index.withOwningConstraintId( getLong( PROP_OWNING_CONSTRAINT, props ) );
        }

        return index;
    }

    private static boolean parseIndexType( String indexRuleType ) throws MalformedSchemaRuleException
    {
        switch ( indexRuleType )
        {
        case "NON_UNIQUE":
            return false;
        case "UNIQUE":
            return true;
        default:
            throw new MalformedSchemaRuleException( "Did not recognize index rule type: " + indexRuleType );
        }
    }

    private static SchemaRule buildConstraintRule( long id, Map<String,Value> props ) throws MalformedSchemaRuleException
    {
        SchemaDescriptor schema = buildSchemaDescriptor( props );
        String constraintRuleType = getString( PROP_CONSTRAINT_RULE_TYPE, props );
        String name = getString( PROP_SCHEMA_RULE_NAME, props );
        OptionalLong ownedIndex = getOptionalLong( PROP_OWNED_INDEX, props );
        ConstraintDescriptor constraint;
        switch ( constraintRuleType )
        {
        case "UNIQUE":
            constraint = ConstraintDescriptorFactory.uniqueForSchema( schema );
            if ( ownedIndex.isPresent() )
            {
                constraint = constraint.withOwnedIndexId( ownedIndex.getAsLong() );
            }
            return constraint.withId( id ).withName( name );
        case "EXISTS":
            constraint = ConstraintDescriptorFactory.existsForSchema( schema );
            return constraint.withId( id ).withName( name );
        case "UNIQUE_EXISTS":
            constraint = ConstraintDescriptorFactory.nodeKeyForSchema( schema );
            if ( ownedIndex.isPresent() )
            {
                constraint = constraint.withOwnedIndexId( ownedIndex.getAsLong() );
            }
            return constraint.withId( id ).withName( name );
        default:
            throw new MalformedSchemaRuleException( "Did not recognize constraint rule type: " + constraintRuleType );
        }
    }

    private static Map<String,Value> schemaRecordToMap( SchemaRecord record, PropertyStore propertyStore, TokenHolders tokenHolders,
            PageCursorTracer cursorTracer ) throws MalformedSchemaRuleException
    {
        Map<String,Value> props = new HashMap<>();
        PropertyRecord propRecord = propertyStore.newRecord();
        long nextProp = record.getNextProp();
        while ( nextProp != NO_NEXT_PROPERTY.longValue() )
        {
            try
            {
                propertyStore.getRecord( nextProp, propRecord, RecordLoad.NORMAL, cursorTracer );
            }
            catch ( InvalidRecordException e )
            {
                throw new MalformedSchemaRuleException(
                        "Cannot read schema rule because it is referencing a property record (id " + nextProp + ") that is invalid: " + propRecord, e );
            }
            for ( PropertyBlock propertyBlock : propRecord )
            {
                PropertyKeyValue propertyKeyValue = propertyBlock.newPropertyKeyValue( propertyStore, cursorTracer );
                insertPropertyIntoMap( propertyKeyValue, props, tokenHolders );
            }
            nextProp = propRecord.getNextProp();
        }
        return props;
    }

    private static void insertPropertyIntoMap( PropertyKeyValue propertyKeyValue, Map<String,Value> props, TokenHolders tokenHolders )
            throws MalformedSchemaRuleException
    {
        try
        {
            NamedToken propertyKeyTokenName = tokenHolders.propertyKeyTokens().getInternalTokenById( propertyKeyValue.propertyKeyId() );
            props.put( propertyKeyTokenName.name(), propertyKeyValue.value() );
        }
        catch ( TokenNotFoundException | InvalidRecordException e )
        {
            int id = propertyKeyValue.propertyKeyId();
            throw new MalformedSchemaRuleException(
                    "Cannot read schema rule because it is referring to a property key token (id " + id + ") that does not exist.", e );
        }
    }

    private static SchemaDescriptor buildSchemaDescriptor( Map<String,Value> props ) throws MalformedSchemaRuleException
    {
        EntityType entityType = getEntityType( getString( PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE, props ) );
        PropertySchemaType propertySchemaType = getPropertySchemaType( getString( PROP_SCHEMA_DESCRIPTOR_PROPERTY_SCHEMA_TYPE, props ) );
        int[] entityIds = getIntArray( PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS, props );
        int[] propertyIds = getIntArray( PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS, props );

        return new SchemaDescriptorImplementation( entityType, propertySchemaType, entityIds, propertyIds );
    }

    private static IndexConfig extractIndexConfig( Map<String,Value> props )
    {
        Map<String,Value> configMap = new HashMap<>();
        for ( Map.Entry<String,Value> entry : props.entrySet() )
        {
            if ( entry.getKey().startsWith( PROP_INDEX_CONFIG_PREFIX ) )
            {
                configMap.put( entry.getKey().substring( PROP_INDEX_CONFIG_PREFIX.length() ), entry.getValue() );
            }
        }
        return IndexConfig.with( configMap );
    }

    private static IndexType getIndexType( String indexType ) throws MalformedSchemaRuleException
    {
        try
        {
            return IndexType.valueOf( indexType );
        }
        catch ( Exception e )
        {
            throw new MalformedSchemaRuleException( "Did not recognize index type: " + indexType, e );
        }
    }

    private static PropertySchemaType getPropertySchemaType( String propertySchemaType ) throws MalformedSchemaRuleException
    {
        try
        {
            return PropertySchemaType.valueOf( propertySchemaType );
        }
        catch ( Exception e )
        {
            throw new MalformedSchemaRuleException( "Did not recognize property schema type: " + propertySchemaType, e );
        }
    }

    private static EntityType getEntityType( String entityType ) throws MalformedSchemaRuleException
    {
        try
        {
            return EntityType.valueOf( entityType );
        }
        catch ( Exception e )
        {
            throw new MalformedSchemaRuleException( "Did not recognize entity type: " + entityType, e );
        }
    }

    private static int[] getIntArray( String property, Map<String,Value> props ) throws MalformedSchemaRuleException
    {
        Value value = props.get( property );
        if ( value instanceof IntArray )
        {
            return (int[]) value.asObject();
        }
        throw new MalformedSchemaRuleException( "Expected property " + property + " to be a IntArray but was " + value );
    }

    private static long getLong( String property, Map<String,Value> props ) throws MalformedSchemaRuleException
    {
        Value value = props.get( property );
        if ( value instanceof LongValue )
        {
            return ((LongValue) value).value();
        }
        throw new MalformedSchemaRuleException( "Expected property " + property + " to be a LongValue but was " + value );
    }

    private static OptionalLong getOptionalLong( String property, Map<String,Value> props )
    {
        Value value = props.get( property );
        if ( value instanceof LongValue )
        {
            return OptionalLong.of( ((LongValue) value).value() );
        }
        return OptionalLong.empty();
    }

    private static String getString( String property, Map<String,Value> map ) throws MalformedSchemaRuleException
    {
        Value value = map.get( property );
        if ( value instanceof TextValue )
        {
            return ((TextValue) value).stringValue();
        }
        throw new MalformedSchemaRuleException( "Expected property " + property + " to be a TextValue but was " + value );
    }

    private static void putLongProperty( Map<String,Value> map, String property, long value )
    {
        map.put( property, Values.longValue( value ) );
    }

    private static void putIntArrayProperty( Map<String,Value> map, String property, int[] value )
    {
        map.put( property, Values.intArray( value ) );
    }

    private static void putStringProperty( Map<String,Value> map, String property, String value )
    {
        map.put( property, Values.utf8Value( value ) );
    }

    private static void putIndexConfigProperty( Map<String,Value> map, String key, Value value )
    {
        map.put( PROP_INDEX_CONFIG_PREFIX + key, value );
    }
}
