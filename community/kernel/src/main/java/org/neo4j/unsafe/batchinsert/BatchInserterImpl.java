/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.batchinsert;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.function.primitive.FunctionFromPrimitiveLong;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.coreapi.schema.BaseConstraintCreator;
import org.neo4j.kernel.impl.coreapi.schema.IndexCreatorImpl;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.coreapi.schema.InternalSchemaActions;
import org.neo4j.kernel.impl.coreapi.schema.PropertyUniqueConstraintDefinition;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.xa.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreProvider;
import org.neo4j.kernel.impl.nioneo.xa.PropertyCreator;
import org.neo4j.kernel.impl.nioneo.xa.PropertyDeleter;
import org.neo4j.kernel.impl.nioneo.xa.PropertyTraverser;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.nioneo.xa.RelationshipCreator;
import org.neo4j.kernel.impl.nioneo.xa.RelationshipGroupGetter;
import org.neo4j.kernel.impl.nioneo.xa.RelationshipLocker;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;

import static java.lang.Boolean.parseBoolean;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.map;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.encodeString;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class BatchInserterImpl implements BatchInserter
{
    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();

    private final Function<RelationshipRecord,Long> REL_RECORD_TO_ID = new Function<RelationshipRecord,Long>()
    {
        @Override
        public Long apply( RelationshipRecord relRecord )
        {
            return relRecord.getId();
        }
    };
    private final Function<RelationshipRecord,BatchRelationship> REL_RECORD_TO_BATCH_REL =
            new Function<RelationshipRecord,BatchRelationship>()
    {
        @Override
        public BatchRelationship apply( RelationshipRecord relRecord )
        {
            RelationshipType type = new RelationshipTypeImpl(
                    relationshipTypeTokens.nameOf( relRecord.getType() ) );
            return new BatchRelationship( relRecord.getId(),
                    relRecord.getFirstNode(), relRecord.getSecondNode(), type );
        }
    };

    private final LifeSupport life;
    private final NeoStore neoStore;
    private final IndexStore indexStore;
    private final File storeDir;
    private final BatchTokenHolder propertyKeyTokens;
    private final BatchTokenHolder relationshipTypeTokens;
    private final BatchTokenHolder labelTokens;
    private final IdGeneratorFactory idGeneratorFactory;
    private final SchemaIndexProviderMap schemaIndexProviders;
    private final LabelScanStore labelScanStore;
    // TODO use Logging instead
    private final StringLogger msgLog;
    private final Logging logging;
    private final FileSystemAbstraction fileSystem;
    private final SchemaCache schemaCache;
    private final Config config;
    private final BatchInserterImpl.BatchSchemaActions actions;
    private final StoreLocker storeLocker;
    private boolean labelsTouched;

    private final FunctionFromPrimitiveLong<Label> labelIdToLabelFunction = new FunctionFromPrimitiveLong<Label>()
    {
        @Override
        public Label apply( long from )
        {
            return label( labelTokens.nameOf( safeCastLongToInt( from ) ) );
        }
    };

    private boolean isShutdown = false;

    // Helper structure for setNodeProperty
    private final RelationshipCreator relationshipCreator;
    private final DirectRecordAccessSet recordAccess;
    private final PropertyTraverser propertyTraverser;
    private final PropertyCreator propertyCreator;
    private final PropertyDeleter propertyDeletor;

    BatchInserterImpl( String storeDir,
                       Map<String, String> stringParams )
    {
        this( storeDir,
              new DefaultFileSystemAbstraction(),
              stringParams,
              Collections.<KernelExtensionFactory<?>>emptyList()
        );
    }

    BatchInserterImpl( String storeDir, FileSystemAbstraction fileSystem,
                       Map<String, String> stringParams, Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        life = new LifeSupport();
        this.fileSystem = fileSystem;
        this.storeDir = new File( FileUtils.fixSeparatorsInPath( storeDir ) );

        rejectAutoUpgrade( stringParams );
        msgLog = StringLogger.loggerDirectory( fileSystem, this.storeDir );
        logging = new SingleLoggingService( msgLog );
        Map<String, String> params = getDefaultParams();
        params.put( GraphDatabaseSettings.use_memory_mapped_buffers.name(), Settings.FALSE );
        params.put( InternalAbstractGraphDatabase.Configuration.store_dir.name(), storeDir );
        params.putAll( stringParams );

        storeLocker = new StoreLocker( fileSystem );
        storeLocker.checkLock( this.storeDir );

        config = new Config( params, GraphDatabaseSettings.class );
        boolean dump = config.get( GraphDatabaseSettings.dump_configuration );
        this.idGeneratorFactory = new DefaultIdGeneratorFactory();

        StoreFactory sf = new StoreFactory( config, idGeneratorFactory, new DefaultWindowPoolFactory(), fileSystem,
                                            msgLog, null );

        File store = fixPath( this.storeDir, sf );

        if ( dump )
        {
            dumpConfiguration( params );
        }
        msgLog.logMessage( Thread.currentThread() + " Starting BatchInserter(" + this + ")" );
        neoStore = sf.newNeoStore( store );
        if ( !neoStore.isStoreOk() )
        {
            throw new IllegalStateException( storeDir + " store is not cleanly shutdown." );
        }
        neoStore.makeStoreOk();
        Token[] indexes = getPropertyKeyTokenStore().getTokens( 10000 );
        propertyKeyTokens = new BatchTokenHolder( indexes );
        labelTokens = new BatchTokenHolder( neoStore.getLabelTokenStore().getTokens( Integer.MAX_VALUE ) );
        Token[] types = getRelationshipTypeStore().getTokens( Integer.MAX_VALUE );
        relationshipTypeTokens = new BatchTokenHolder( types );
        indexStore = life.add( new IndexStore( this.storeDir, fileSystem ) );
        schemaCache = new SchemaCache( neoStore.getSchemaStore() );

        KernelExtensions extensions = life
                .add( new KernelExtensions( kernelExtensions, config, new DependencyResolverImpl(),
                                            UnsatisfiedDependencyStrategies.ignore() ) );

        life.start();

        SchemaIndexProvider provider = extensions.resolveDependency( SchemaIndexProvider.class,
                SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE );
        schemaIndexProviders = new DefaultSchemaIndexProviderMap( provider );
        labelScanStore = life.add( extensions.resolveDependency( LabelScanStoreProvider.class,
                LabelScanStoreProvider.HIGHEST_PRIORITIZED ).getLabelScanStore() );
        actions = new BatchSchemaActions();

        // Record access
        recordAccess = new DirectRecordAccessSet( neoStore );
        relationshipCreator = new RelationshipCreator( RelationshipLocker.NO_LOCKING,
                new RelationshipGroupGetter( neoStore.getRelationshipGroupStore() ), neoStore.getDenseNodeThreshold() );
        propertyTraverser = new PropertyTraverser();
        propertyCreator = new PropertyCreator( getPropertyStore(), propertyTraverser );
        propertyDeletor = new PropertyDeleter( getPropertyStore(), propertyTraverser );
    }

    private Map<String, String> getDefaultParams()
    {
        Map<String, String> params = new HashMap<>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "50M" );
        params.put( "neostore.relationshipgroupstore.db.mapped_memory", "10M" );
        return params;
    }

    @Override
    public boolean nodeHasProperty( long node, String propertyName )
    {
        return primitiveHasProperty( getNodeRecord( node ).forChangingData(), propertyName );
    }

    @Override
    public boolean relationshipHasProperty( long relationship, String propertyName )
    {
        return primitiveHasProperty(
                recordAccess.getRelRecords().getOrLoad( relationship, null ).forReadingData(), propertyName );
    }

    @Override
    public void setNodeProperty( long node, String propertyName, Object newValue )
    {
        propertyCreator.setPrimitiveProperty( getNodeRecord( node ), getOrCreatePropertyKeyId( propertyName ),
                newValue, recordAccess.getPropertyRecords() );
        recordAccess.commit();
    }

    @Override
    public void setRelationshipProperty( long relationship, String propertyName, Object propertyValue )
    {
        propertyCreator.setPrimitiveProperty( getRelationshipRecord( relationship ),
                getOrCreatePropertyKeyId( propertyName ), propertyValue, recordAccess.getPropertyRecords() );
        recordAccess.commit();
    }

    @Override
    public void removeNodeProperty( long node, String propertyName )
    {
        int propertyKey = getOrCreatePropertyKeyId( propertyName );
        propertyDeletor.removeProperty( getNodeRecord( node ), propertyKey, recordAccess.getPropertyRecords() );
        recordAccess.commit();
    }

    @Override
    public void removeRelationshipProperty( long relationship,
                                            String propertyName )
    {
        int propertyKey = getOrCreatePropertyKeyId( propertyName );
        propertyDeletor.removeProperty( getRelationshipRecord( relationship ), propertyKey,
                recordAccess.getPropertyRecords() );
        recordAccess.commit();
    }

    @Override
    public IndexCreator createDeferredSchemaIndex( Label label )
    {
        return new IndexCreatorImpl( actions, label );
    }

    private void checkSchemaCreationConstraints( int labelId, int propertyKeyId )
    {
        for ( SchemaRule rule : schemaCache.schemaRulesForLabel( labelId ) )
        {
            int otherPropertyKeyId;

            switch ( rule.getKind() )
            {
                case INDEX_RULE:
                case CONSTRAINT_INDEX_RULE:
                    otherPropertyKeyId = ((IndexRule) rule).getPropertyKey();
                    break;
                case UNIQUENESS_CONSTRAINT:
                    otherPropertyKeyId = ((UniquenessConstraintRule) rule).getPropertyKey();
                    break;
                default:
                    throw new IllegalStateException( "Case not handled.");
            }

            if ( otherPropertyKeyId == propertyKeyId )
            {
                throw new ConstraintViolationException(
                        "It is not allowed to create schema constraints and indexes on the same {label;property}." );
            }
        }
    }

    private void createIndexRule( int labelId, int propertyKeyId )
    {
        SchemaStore schemaStore = getSchemaStore();
        IndexRule schemaRule = IndexRule.indexRule( schemaStore.nextId(), labelId, propertyKeyId,
                                                    this.schemaIndexProviders.getDefaultProvider()
                                                                             .getProviderDescriptor() );
        for ( DynamicRecord record : schemaStore.allocateFrom( schemaRule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( schemaRule );
        labelsTouched = true;
        recordAccess.commit();
    }

    private void repopulateAllIndexes() throws IOException, IndexCapacityExceededException
    {
        if ( !labelsTouched )
        {
            return;
        }

        final IndexRule[] rules = getIndexesNeedingPopulation();
        final IndexPopulator[] populators = new IndexPopulator[rules.length];
        // the store is uncontended at this point, so creating a local LockService is safe.
        LockService locks = new ReentrantLockService();
        IndexStoreView storeView = new NeoStoreIndexStoreView( locks, neoStore );

        final int[] labelIds = new int[rules.length];
        final int[] propertyKeyIds = new int[rules.length];

        for ( int i = 0; i < labelIds.length; i++ )
        {
            IndexRule rule = rules[i];
            int labelId = rule.getLabel();
            int propertyKeyId = rule.getPropertyKey();
            labelIds[i] = labelId;
            propertyKeyIds[i] = propertyKeyId;

            IndexDescriptor descriptor = new IndexDescriptor( labelId, propertyKeyId );
            populators[i] = schemaIndexProviders.apply( rule.getProviderDescriptor() ).getPopulator(
                    rule.getId(), descriptor, new IndexConfiguration( rule.isConstraintIndex() ) );
            populators[i].create();
        }

        Visitor<NodePropertyUpdate, IOException> propertyUpdateVisitor = new Visitor<NodePropertyUpdate, IOException>()
        {
            @Override
            public boolean visit( NodePropertyUpdate update ) throws IOException
            {
                // Do a lookup from which property has changed to a list of indexes worried about that property.
                int propertyKeyInQuestion = update.getPropertyKeyId();
                for ( int i = 0; i < propertyKeyIds.length; i++ )
                {
                    if ( propertyKeyIds[i] == propertyKeyInQuestion )
                    {
                        if ( update.forLabel( labelIds[i] ) )
                        {
                            try
                            {
                                populators[i].add( update.getNodeId(), update.getValueAfter() );
                            }
                            catch ( IndexEntryConflictException conflict )
                            {
                                throw conflict.notAllowed( rules[i].getLabel(), rules[i].getPropertyKey() );
                            }
                            catch ( IndexCapacityExceededException e )
                            {
                                throw new UnderlyingStorageException( e );
                            }
                        }
                    }
                }
                return true;
            }
        };

        InitialNodeLabelCreationVisitor labelUpdateVisitor = new InitialNodeLabelCreationVisitor();
        StoreScan<IOException> storeScan = storeView.visitNodes( labelIds, propertyKeyIds,
                propertyUpdateVisitor, labelUpdateVisitor );
        storeScan.run();

        for ( IndexPopulator populator : populators )
        {
            populator.close( true );
        }
        labelUpdateVisitor.close();
    }

    private class InitialNodeLabelCreationVisitor implements Visitor<NodeLabelUpdate, IOException>
    {
        LabelScanWriter writer = labelScanStore.newWriter();

        @Override
        public boolean visit( NodeLabelUpdate update ) throws IOException
        {
            try
            {
                writer.write( update );
            }
            catch ( IndexCapacityExceededException e )
            {
                throw new UnderlyingStorageException( e );
            }
            return true;
        }

        public void close() throws IOException
        {
            writer.close();
        }
    }

    private IndexRule[] getIndexesNeedingPopulation()
    {
        List<IndexRule> indexesNeedingPopulation = new ArrayList<>();
        for ( SchemaRule rule : schemaCache.schemaRules() )
        {
            if ( rule.getKind().isIndex() )
            {
                IndexRule indexRule = (IndexRule) rule;
                SchemaIndexProvider provider =
                        schemaIndexProviders.apply( indexRule.getProviderDescriptor() );
                if ( provider.getInitialState( indexRule.getId() ) != InternalIndexState.FAILED )
                {
                    indexesNeedingPopulation.add( indexRule );
                }
            }
        }
        return indexesNeedingPopulation.toArray( new IndexRule[indexesNeedingPopulation.size()] );
    }

    @Override
    public ConstraintCreator createDeferredConstraint( Label label )
    {
        return new BaseConstraintCreator( new BatchSchemaActions(), label );
    }

    private void createConstraintRule( UniquenessConstraint constraint )
    {
        // TODO: Do not create duplicate index

        SchemaStore schemaStore = getSchemaStore();

        long indexRuleId = schemaStore.nextId();
        long constraintRuleId = schemaStore.nextId();

        IndexRule indexRule = IndexRule.constraintIndexRule(
                indexRuleId, constraint.label(), constraint.propertyKeyId(),
                this.schemaIndexProviders.getDefaultProvider().getProviderDescriptor(),
                constraintRuleId );
        UniquenessConstraintRule constraintRule = UniquenessConstraintRule.uniquenessConstraintRule(
                constraintRuleId, constraint.label(), constraint.propertyKeyId(), indexRuleId );

        for ( DynamicRecord record : schemaStore.allocateFrom( constraintRule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( constraintRule );
        for ( DynamicRecord record : schemaStore.allocateFrom( indexRule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( indexRule );
        labelsTouched = true;
        recordAccess.commit();
    }

    private int getOrCreatePropertyKeyId( String name )
    {
        int propertyKeyId = getPropertyKeyId( name );
        if ( propertyKeyId == -1 )
        {
            propertyKeyId = createNewPropertyKeyId( name );
        }
        return propertyKeyId;
    }

    private int getOrCreateRelationshipTypeToken( RelationshipType type )
    {
        int typeId = relationshipTypeTokens.idOf( type.name() );
        if ( typeId == -1 )
        {
            typeId = createNewRelationshipType( type.name() );
        }
        return typeId;
    }

    private int getPropertyKeyId( String name )
    {
        return propertyKeyTokens.idOf( name );
    }

    private int getOrCreateLabelId( String name )
    {
        int labelId = getLabelId( name );
        if ( labelId == -1 )
        {
            labelId = createNewLabelId( name );
        }
        return labelId;
    }

    private int getLabelId( String name )
    {
        return labelTokens.idOf( name );
    }

    private boolean primitiveHasProperty( PrimitiveRecord record,
                                          String propertyName )
    {
        int propertyKeyId = propertyKeyTokens.idOf( propertyName );
        return propertyKeyId != -1 && propertyTraverser.findPropertyRecordContaining( record, propertyKeyId,
                recordAccess.getPropertyRecords(), false ) != Record.NO_NEXT_PROPERTY.intValue();
    }

    private void rejectAutoUpgrade( Map<String, String> params )
    {
        if ( parseBoolean( params.get( GraphDatabaseSettings.allow_store_upgrade.name() ) ) )
        {
            throw new IllegalArgumentException( "Batch inserter is not allowed to do upgrade of a store" +
                                                ", use " + EmbeddedGraphDatabase.class.getSimpleName() + " instead" );
        }
    }

    @Override
    public long createNode( Map<String, Object> properties, Label... labels )
    {
        return internalCreateNode( getNodeStore().nextId(), properties, labels );
    }

    private long internalCreateNode( long nodeId, Map<String, Object> properties, Label... labels )
    {
        NodeRecord nodeRecord = recordAccess.getNodeRecords().create( nodeId, null ).forChangingData();
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
        nodeRecord.setNextProp( propertyCreator.createPropertyChain( nodeRecord,
                propertiesIterator( properties ), recordAccess.getPropertyRecords() ) );

        if ( labels.length > 0 )
        {
            setNodeLabels( nodeRecord, labels );
        }

        recordAccess.commit();
        return nodeId;
    }

    private Iterator<PropertyBlock> propertiesIterator( Map<String, Object> properties )
    {
        if ( properties == null || properties.isEmpty() )
        {
            return IteratorUtil.emptyIterator();
        }
        return new IteratorWrapper<PropertyBlock, Map.Entry<String,Object>>( properties.entrySet().iterator() )
        {
            @Override
            protected PropertyBlock underlyingObjectToObject( Entry<String, Object> property )
            {
                return propertyCreator.encodePropertyValue(
                        getOrCreatePropertyKeyId( property.getKey() ), property.getValue() );
            }
        };
    }

    private void setNodeLabels( NodeRecord nodeRecord, Label... labels )
    {
        NodeLabels nodeLabels = parseLabelsField( nodeRecord );
        getNodeStore().updateDynamicLabelRecords( nodeLabels.put( getOrCreateLabelIds( labels ), getNodeStore(),
                getNodeStore().getDynamicLabelStore() ) );
        labelsTouched = true;
    }

    private long[] getOrCreateLabelIds( Label[] labels )
    {
        long[] ids = new long[labels.length];
        int cursor = 0;
        for ( int i = 0; i < ids.length; i++ )
        {
            int labelId = getOrCreateLabelId( labels[i].name() );
            if ( !arrayContains( ids, cursor, labelId ) )
            {
                ids[cursor++] = labelId;
            }
        }
        if ( cursor < ids.length )
        {
            ids = Arrays.copyOf( ids, cursor );
        }
        return ids;
    }

    private boolean arrayContains( long[] ids, int cursor, int labelId )
    {
        for ( int i = 0; i < cursor; i++ )
        {
            if ( ids[i] == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void createNode( long id, Map<String, Object> properties, Label... labels )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new IllegalArgumentException( "id=" + id );
        }
        if ( id == IdGeneratorImpl.INTEGER_MINUS_ONE )
        {
            throw new IllegalArgumentException( "id " + id + " is reserved for internal use" );
        }
        NodeStore nodeStore = neoStore.getNodeStore();
        if ( neoStore.getNodeStore().loadLightNode( id ) != null )
        {
            throw new IllegalArgumentException( "id=" + id + " already in use" );
        }
        long highId = nodeStore.getHighId();
        if ( highId <= id )
        {
            nodeStore.setHighId( id + 1 );
        }
        internalCreateNode( id, properties, labels );
    }

    @Override
    public void setNodeLabels( long node, Label... labels )
    {
        NodeRecord record = getNodeRecord( node ).forChangingData();
        setNodeLabels( record, labels );
        recordAccess.commit();
    }

    @Override
    public Iterable<Label> getNodeLabels( final long node )
    {
        return new Iterable<Label>()
        {
            @Override
            public Iterator<Label> iterator()
            {
                NodeRecord record = getNodeRecord( node ).forReadingData();
                long[] labels = parseLabelsField( record ).get( getNodeStore() );
                return map( labelIdToLabelFunction, PrimitiveLongCollections.iterator( labels ) );
            }
        };
    }

    @Override
    public boolean nodeHasLabel( long node, Label label )
    {
        int labelId = getLabelId( label.name() );
        return labelId != -1 && nodeHasLabel( node, labelId );
    }

    private boolean nodeHasLabel( long node, int labelId )
    {
        NodeRecord record = getNodeRecord( node ).forReadingData();
        for ( long label : parseLabelsField( record ).get( getNodeStore() ) )
        {
            if ( label == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public long createRelationship( long node1, long node2, RelationshipType type,
            Map<String, Object> properties )
    {
        long id = neoStore.getRelationshipStore().nextId();
        int typeId = getOrCreateRelationshipTypeToken( type );
        relationshipCreator.relationshipCreate( id, typeId, node1, node2, recordAccess );
        if ( properties != null && !properties.isEmpty() )
        {
            RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( id, null ).forChangingData();
            record.setNextProp( propertyCreator.createPropertyChain( record,
                    propertiesIterator( properties ), recordAccess.getPropertyRecords() ) );
        }
        recordAccess.commit();
        return id;
    }

    @Override
    public void setNodeProperties( long node, Map<String, Object> properties )
    {
        NodeRecord record = getNodeRecord( node ).forChangingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            propertyDeletor.getAndDeletePropertyChain( record, recordAccess.getPropertyRecords() );
        }
        record.setNextProp( propertyCreator.createPropertyChain( record, propertiesIterator( properties ),
                recordAccess.getPropertyRecords() ) );
        recordAccess.commit();
    }

    @Override
    public void setRelationshipProperties( long rel, Map<String, Object> properties )
    {
        RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( rel, null ).forChangingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            propertyDeletor.getAndDeletePropertyChain( record, recordAccess.getPropertyRecords() );
        }
        record.setNextProp( propertyCreator.createPropertyChain( record, propertiesIterator( properties ),
                recordAccess.getPropertyRecords() ) );
        recordAccess.commit();
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        return neoStore.getNodeStore().loadLightNode( nodeId ) != null;
    }

    @Override
    public Map<String, Object> getNodeProperties( long nodeId )
    {
        NodeRecord record = getNodeRecord( nodeId ).forReadingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    @Override
    public Iterable<Long> getRelationshipIds( long nodeId )
    {
        return map( REL_RECORD_TO_ID, new BatchRelationshipIterable( neoStore, nodeId ) );
    }

    @Override
    public Iterable<BatchRelationship> getRelationships( long nodeId )
    {
        return map( REL_RECORD_TO_BATCH_REL, new BatchRelationshipIterable( neoStore, nodeId ) );
    }

    @Override
    public BatchRelationship getRelationshipById( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId ).forReadingData();
        RelationshipType type = new RelationshipTypeImpl(
                relationshipTypeTokens.nameOf( record.getType() ) );
        return new BatchRelationship( record.getId(), record.getFirstNode(),
                                      record.getSecondNode(), type );
    }

    @Override
    public Map<String, Object> getRelationshipProperties( long relId )
    {
        RelationshipRecord record = recordAccess.getRelRecords().getOrLoad( relId, null ).forChangingData();
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    @Override
    public void shutdown()
    {
        recordAccess.close();

        if ( isShutdown )
        {
            throw new IllegalStateException( "Batch inserter already has shutdown" );
        }
        isShutdown = true;

        try
        {
            repopulateAllIndexes();
        }
        catch ( IOException | IndexCapacityExceededException e )
        {
            throw new RuntimeException( e );
        }
        neoStore.close();

        try
        {
            storeLocker.release();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Could not release store lock", e );
        }

        msgLog.logMessage( Thread.currentThread() + " Clean shutdown on BatchInserter(" + this + ")", true );
        msgLog.close();
        life.shutdown();
    }

    @Override
    public String toString()
    {
        return "EmbeddedBatchInserter[" + storeDir + "]";
    }

    private static class RelationshipTypeImpl implements RelationshipType
    {
        private final String name;

        RelationshipTypeImpl( String name )
        {
            this.name = name;
        }

        @Override
        public String name()
        {
            return name;
        }
    }

    private Map<String, Object> getPropertyChain( long nextProp )
    {
        final Map<String, Object> map = new HashMap<>();
        propertyTraverser.getPropertyChain( nextProp, recordAccess.getPropertyRecords(), new Listener<PropertyBlock>()
        {
            @Override
            public void receive( PropertyBlock propBlock )
            {
                String key = propertyKeyTokens.nameOf( propBlock.getKeyIndexId() );
                DefinedProperty propertyData = propBlock.newPropertyData( getPropertyStore() );
                Object value = propertyData.value() != null ? propertyData.value() :
                    propBlock.getType().getValue( propBlock, getPropertyStore() );
                map.put( key, value );
            }
        } );
        return map;
    }

    private int createNewPropertyKeyId( String stringKey )
    {
        PropertyKeyTokenStore idxStore = getPropertyKeyTokenStore();
        int keyId = (int) idxStore.nextId();
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( keyId );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> keyRecords =
                idxStore.allocateNameRecords( encodeString( stringKey ) );
        record.setNameId( (int) first( keyRecords ).getId() );
        record.addNameRecords( keyRecords );
        idxStore.updateRecord( record );
        propertyKeyTokens.addToken( stringKey, keyId );
        return keyId;
    }

    private int createNewLabelId( String stringKey )
    {
        LabelTokenStore labelTokenStore = neoStore.getLabelTokenStore();
        int keyId = (int) labelTokenStore.nextId();
        LabelTokenRecord record = new LabelTokenRecord( keyId );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> keyRecords =
                labelTokenStore.allocateNameRecords( encodeString( stringKey ) );
        record.setNameId( (int) first( keyRecords ).getId() );
        record.addNameRecords( keyRecords );
        labelTokenStore.updateRecord( record );
        labelTokens.addToken( stringKey, keyId );
        return keyId;
    }

    private int createNewRelationshipType( String name )
    {
        RelationshipTypeTokenStore typeStore = getRelationshipTypeStore();
        int id = (int) typeStore.nextId();
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        record.setInUse( true );
        record.setCreated();
        Collection<DynamicRecord> nameRecords = typeStore.allocateNameRecords( encodeString( name ) );
        record.setNameId( (int) first( nameRecords ).getId() );
        record.addNameRecords( nameRecords );
        typeStore.updateRecord( record );
        relationshipTypeTokens.addToken( name, id );
        return id;
    }

    private NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    private RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    private PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return neoStore.getPropertyKeyTokenStore();
    }

    private RelationshipTypeTokenStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    private SchemaStore getSchemaStore()
    {
        return neoStore.getSchemaStore();
    }

    private RecordProxy<Long,NodeRecord,Void> getNodeRecord( long id )
    {
        if ( id < 0 || id >= getNodeStore().getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return recordAccess.getNodeRecords().getOrLoad( id, null );
    }

    private RecordProxy<Long,RelationshipRecord,Void> getRelationshipRecord( long id )
    {
        if ( id < 0 || id >= getRelationshipStore().getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return recordAccess.getRelRecords().getOrLoad( id, null );
    }

    private File fixPath( File dir, StoreFactory sf )
    {
        try
        {
            fileSystem.mkdirs( dir );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to create directory path["
                    + storeDir + "] for Neo4j kernel store." );
        }

        File store = new File( dir, NeoStore.DEFAULT_NAME );
        if ( !fileSystem.fileExists( store ) )
        {
            sf.createNeoStore( store ).close();
        }
        return store;
    }

    @Override
    public String getStoreDir()
    {
        return storeDir.getPath();
    }

    // needed by lucene-index
    public IndexStore getIndexStore()
    {
        return this.indexStore;
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }

    private void dumpConfiguration( Map<String, String> config )
    {
        for ( String key : config.keySet() )
        {
            Object value = config.get( key );
            if ( value != null )
            {
                System.out.println( key + "=" + value );
            }
        }
    }

    private class BatchSchemaActions implements InternalSchemaActions
    {
        @Override
        public IndexDefinition createIndexDefinition( Label label, String propertyKey )
        {
            int labelId = getOrCreateLabelId( label.name() );
            int propertyKeyId = getOrCreatePropertyKeyId( propertyKey );

            checkSchemaCreationConstraints( labelId, propertyKeyId );

            createIndexRule( labelId, propertyKeyId );
            return new IndexDefinitionImpl( this, label, propertyKey, false );
        }

        @Override
        public void dropIndexDefinitions( Label label, String propertyKey )
        {
            throw unsupportedException();
        }

        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( Label label, String propertyKey )
        {
            int labelId = getOrCreateLabelId( label.name() );
            int propertyKeyId = getOrCreatePropertyKeyId( propertyKey );

            checkSchemaCreationConstraints( labelId, propertyKeyId );

            createConstraintRule( new UniquenessConstraint( labelId, propertyKeyId ) );
            return new PropertyUniqueConstraintDefinition( this, label, propertyKey );
        }

        @Override
        public void dropPropertyUniquenessConstraint( Label label, String propertyKey )
        {
            throw unsupportedException();
        }

        @Override
        public String getUserMessage( KernelException e )
        {
            throw unsupportedException();
        }

        @Override
        public void assertInTransaction()
        {
            // BatchInserterImpl always is expected to be running in one big single "transaction"
        }

        private UnsupportedOperationException unsupportedException()
        {
            return new UnsupportedOperationException( "Batch inserter doesn't support this" );
        }
    }

    private class DependencyResolverImpl extends DependencyResolver.Adapter
    {
        @Override
        public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
        {
            if ( type.isInstance( fileSystem ) )
            {
                return type.cast( fileSystem );
            }
            if ( type.isInstance( config ) )
            {
                return type.cast( config );
            }
            if ( type.isInstance( logging ) )
            {
                return type.cast( logging );
            }
            if ( NeoStoreProvider.class.isAssignableFrom( type ) )
            {
                return type.cast( new NeoStoreProvider()
                {
                    @Override
                    public NeoStore evaluate()
                    {
                        return neoStore;
                    }
                } );
            }
            throw new IllegalArgumentException( "Unknown dependency " + type );
        }
    }
}
