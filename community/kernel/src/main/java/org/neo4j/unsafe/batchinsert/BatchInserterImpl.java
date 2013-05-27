/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.BaseConstraintCreator;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.IndexCreatorImpl;
import org.neo4j.kernel.IndexDefinitionImpl;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.InternalSchemaActions;
import org.neo4j.kernel.PropertyUniqueConstraintDefinition;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.impl.api.SchemaCache;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
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
import org.neo4j.kernel.impl.nioneo.xa.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.nioneo.xa.NodeLabelRecordLogic;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;

import static java.lang.Boolean.parseBoolean;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE;
import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.encodeString;

public class BatchInserterImpl implements BatchInserter
{
    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();

    private final LifeSupport life;
    private final NeoStore neoStore;
    private final IndexStore indexStore;
    private final File storeDir;
    private final BatchTokenHolder propertyKeyTokens;
    private final BatchTokenHolder relationshipTypeTokens;
    private final BatchTokenHolder labelTokens;
    private final IdGeneratorFactory idGeneratorFactory;
    private final SchemaIndexProviderMap schemaIndexProviders;
    // TODO use Logging instead
    private final StringLogger msgLog;
    private final FileSystemAbstraction fileSystem;
    private final SchemaCache schemaCache;
    private final Config config;
    private boolean isShutdown = false;

    private final Function<Long, Label> labelIdToLabelFunction = new Function<Long, Label>()
    {
        @Override
        public Label apply( Long from )
        {
            return label( labelTokens.nameOf( from.intValue() ) );
        }
    };

    private final BatchInserterImpl.BatchSchemaActions actions;

    BatchInserterImpl( String storeDir, FileSystemAbstraction fileSystem,
                       Map<String, String> stringParams, Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        life = new LifeSupport();
        this.fileSystem = fileSystem;
        this.storeDir = new File( FileUtils.fixSeparatorsInPath( storeDir ) );

        rejectAutoUpgrade( stringParams );
        msgLog = StringLogger.loggerDirectory( fileSystem, this.storeDir );
        Map<String, String> params = getDefaultParams();
        params.put( GraphDatabaseSettings.use_memory_mapped_buffers.name(), Settings.FALSE );
        params.put( InternalAbstractGraphDatabase.Configuration.store_dir.name(), storeDir );
        params.putAll( stringParams );

        config = new Config( params, GraphDatabaseSettings.class );
        boolean dump = config.get( GraphDatabaseSettings.dump_configuration );
        this.idGeneratorFactory = new DefaultIdGeneratorFactory();

        StoreFactory sf = new StoreFactory( config, idGeneratorFactory, new DefaultWindowPoolFactory(), fileSystem,
                                            StringLogger.DEV_NULL, null );

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

        SchemaIndexProvider provider =
                extensions.resolveDependency( SchemaIndexProvider.class, HIGHEST_PRIORITIZED_OR_NONE );
        schemaIndexProviders = new DefaultSchemaIndexProviderMap( provider );
        actions = new BatchSchemaActions();
    }

    private Map<String, String> getDefaultParams()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "50M" );
        return params;
    }

    @Override
    public boolean nodeHasProperty( long node, String propertyName )
    {
        return primitiveHasProperty( getNodeRecord( node ), propertyName );
    }

    @Override
    public boolean relationshipHasProperty( long relationship,
                                            String propertyName )
    {
        return primitiveHasProperty( getRelationshipRecord( relationship ),
                                     propertyName );
    }

    @Override
    public void setNodeProperty( long node, String propertyName,
                                 Object newValue )
    {
        NodeRecord nodeRec = getNodeRecord( node );
        if ( setPrimitiveProperty( nodeRec, propertyName, newValue ) )
        {
            getNodeStore().updateRecord( nodeRec );
        }
    }

    @Override
    public void setRelationshipProperty( long relationship,
                                         String propertyName, Object propertyValue )
    {
        RelationshipRecord relRec = getRelationshipRecord( relationship );
        if ( setPrimitiveProperty( relRec, propertyName, propertyValue ) )
        {
            getRelationshipStore().updateRecord( relRec );
        }
    }

    @Override
    public void removeNodeProperty( long node, String propertyName )
    {
        NodeRecord nodeRec = getNodeRecord( node );
        if ( removePrimitiveProperty( nodeRec, propertyName ) )
        {
            getNodeStore().updateRecord( nodeRec );
        }
    }

    @Override
    public void removeRelationshipProperty( long relationship,
                                            String propertyName )
    {
        RelationshipRecord relationshipRec = getRelationshipRecord( relationship );
        if ( removePrimitiveProperty( relationshipRec, propertyName ) )
        {
            getRelationshipStore().updateRecord( relationshipRec );
        }
    }

    @Override
    public IndexCreator createDeferredSchemaIndex( Label label )
    {
        return new IndexCreatorImpl( actions, label );
    }

    private void createIndexRule( Label label, String propertyKey )
    {
        // TODO: Do not create duplicate index

        SchemaStore schemaStore = getSchemaStore();
        IndexRule schemaRule = IndexRule.indexRule( schemaStore.nextId(), getOrCreateLabelId( label.name() ),
                                                    getOrCreatePropertyKeyId( propertyKey ),
                                                    this.schemaIndexProviders.getDefaultProvider()
                                                                             .getProviderDescriptor() );
        for ( DynamicRecord record : schemaStore.allocateFrom( schemaRule ) )
        {
            schemaStore.updateRecord( record );
        }
        schemaCache.addSchemaRule( schemaRule );
    }

    private void repopulateAllIndexes() throws IOException
    {
        IndexRule[] rules = getIndexesNeedingPopulation();
        final IndexPopulator[] populators = new IndexPopulator[rules.length];
        IndexStoreView storeView = new NeoStoreIndexStoreView( neoStore );

        final long[] labelIds = new long[rules.length];
        final long[] propertyKeyIds = new long[rules.length];

        for ( int i = 0; i < labelIds.length; i++ )
        {
            IndexRule rule = rules[i];
            labelIds[i] = rule.getLabel();
            propertyKeyIds[i] = rule.getPropertyKey();

            populators[i] = schemaIndexProviders.apply( rule.getProviderDescriptor() ).getPopulator(
                    rule.getId(), new IndexConfiguration( rule.isConstraintIndex() ) );
            populators[i].create();
        }

        StoreScan<IOException> storeScan = storeView.visitNodes( labelIds, propertyKeyIds,
                new Visitor<NodePropertyUpdate, IOException>()
        {
            @Override
            public boolean visit( NodePropertyUpdate update ) throws IOException
            {
                int i = indexOf( propertyKeyIds, update.getPropertyKeyId() );
                if ( i == -1 )
                {
                    throw new ThisShouldNotHappenError( "Mattias", "The store view scan gave back a node property " +
                                                                   "update that I didn't care about. I care about these properties:" +
                                                                   Arrays.toString( propertyKeyIds ) + ", but got:" +
                                                                   update.getPropertyKeyId() );
                }

                if ( update.forLabel( labelIds[i] ) )
                {
                    try
                    {
                        populators[i].add( update.getNodeId(), update.getValueAfter() );
                    }
                    catch ( IndexEntryConflictException conflict )
                    {
                        throw conflict.notAllowed( labelIds[i], propertyKeyIds[i] );
                    }
                    return true;
                }
                return false;
            }

            private int indexOf( long[] ids, long idToFind )
            {
                for ( int i = 0; i < ids.length; i++ )
                {
                    if ( ids[i] == idToFind )
                    {
                        return i;
                    }
                }
                return -1;
            }
        } );
        storeScan.run();

        for ( IndexPopulator populator : populators )
        {
            populator.close( true );
        }
    }

    private IndexRule[] getIndexesNeedingPopulation()
    {
        List<IndexRule> indexesNeedingPopulation = new ArrayList<IndexRule>();
        for ( SchemaRule rule : schemaCache.getSchemaRules() )
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
                indexRuleId, constraint.label(), constraint.property(),
                this.schemaIndexProviders.getDefaultProvider().getProviderDescriptor(),
                constraintRuleId );
        UniquenessConstraintRule constraintRule = UniquenessConstraintRule.uniquenessConstraintRule(
                schemaStore.nextId(), constraint.label(), constraint.property(), indexRuleId );

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
    }

    private boolean removePrimitiveProperty( PrimitiveRecord primitive,
                                             String property )
    {
        PropertyRecord current = null;
        PropertyBlock target;
        long nextProp = primitive.getNextProp();
        int propIndex = propertyKeyTokens.idOf( property );
        if ( nextProp == Record.NO_NEXT_PROPERTY.intValue() || propIndex == -1 )
        {
            // No properties or no one has that property, nothing changed
            return false;
        }
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            current = getPropertyStore().getRecord( nextProp );
            if ( (target = current.removePropertyBlock( propIndex )) != null )
            {
                getPropertyStore().ensureHeavy( target );
                for ( DynamicRecord dynRec : target.getValueRecords() )
                {
                    current.addDeletedRecord( dynRec );
                }
                break;
            }
            nextProp = current.getNextProp();
        }
        assert current != null : "the if statement above prevents it";
        if ( current.size() > 0 )
        {
            getPropertyStore().updateRecord( current );
            return false;
        }
        else
        {
            return unlinkPropertyRecord( current, primitive );
        }
    }

    private boolean unlinkPropertyRecord( PropertyRecord propRecord,
                                          PrimitiveRecord primitive )
    {
        assert propRecord.size() == 0;
        boolean primitiveChanged = false;
        long prevProp = propRecord.getPrevProp();
        long nextProp = propRecord.getNextProp();
        if ( primitive.getNextProp() == propRecord.getId() )
        {
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                                                                                        + " for "
                                                                                        + primitive;
            primitive.setNextProp( nextProp );
            primitiveChanged = true;
        }
        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
        {
            PropertyRecord prevPropRecord = getPropertyStore().getRecord(
                    prevProp );
            assert prevPropRecord.inUse() : prevPropRecord + "->" + propRecord
                                            + " for " + primitive;
            prevPropRecord.setNextProp( nextProp );
            getPropertyStore().updateRecord( prevPropRecord );
        }
        if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord nextPropRecord = getPropertyStore().getRecord(
                    nextProp );
            assert nextPropRecord.inUse() : propRecord + "->" + nextPropRecord
                                            + " for " + primitive;
            nextPropRecord.setPrevProp( prevProp );
            getPropertyStore().updateRecord( nextPropRecord );
        }
        propRecord.setInUse( false );
        /*
         *  The following two are not needed - the above line does all the work (PropertyStore
         *  does not write out the prev/next for !inUse records). It is nice to set this
         *  however to check for consistency when assertPropertyChain().
         */
        propRecord.setPrevProp( Record.NO_PREVIOUS_PROPERTY.intValue() );
        propRecord.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
        getPropertyStore().updateRecord( propRecord );
        return primitiveChanged;
    }

    /** @return true if the passed primitive needs updating in the store. */
    private boolean setPrimitiveProperty( PrimitiveRecord primitive,
                                          String name,
                                          Object value )
    {
        boolean result = false;
        long nextProp = primitive.getNextProp();
        int index = getOrCreatePropertyKeyId( name );
        PropertyBlock block = new PropertyBlock();
        getPropertyStore().encodeValue( block, index, value );
        int size = block.getSize();

        /*
         * current is the current record traversed
         * thatFits is the earliest record that can host the block
         * thatHas is the record that already has a block for this index
         */
        PropertyRecord current, thatFits = null, thatHas = null;
        /*
         * We keep going while there are records or until we both found the
         * property if it exists and the place to put it, if exists.
         */
        while ( !(nextProp == Record.NO_NEXT_PROPERTY.intValue() || (thatHas != null && thatFits != null)) )
        {
            current = getPropertyStore().getRecord( nextProp );
            /*
             * current.getPropertyBlock() is cheap but not free. If we already
             * have found thatHas, then we can skip this lookup.
             */
            if ( thatHas == null && current.getPropertyBlock( index ) != null )
            {
                thatHas = current;
                PropertyBlock removed = thatHas.removePropertyBlock( index );
                getPropertyStore().ensureHeavy( removed );
                for ( DynamicRecord dynRec : removed.getValueRecords() )
                {
                    thatHas.addDeletedRecord( dynRec );
                }
                getPropertyStore().updateRecord( thatHas );
            }
            /*
             * We check the size after we remove - potentially we can put in the same record.
             *
             * current.size() is cheap but not free. If we already found somewhere
             * where it fits, no need to look again.
             */
            if ( thatFits == null
                 && (PropertyType.getPayloadSize() - current.size() >= size) )
            {
                thatFits = current;
            }
            nextProp = current.getNextProp();
        }
        /*
         * thatHas is of no importance here. We know that the block is definitely not there.
         * However, we can be sure that if the property existed, thatHas is not null and does
         * not contain the block.
         *
         * thatFits is interesting. If null, we need to create a new record and link, otherwise
         * just add the block there.
         */
        if ( thatFits == null )
        {
            thatFits = new PropertyRecord( getPropertyStore().nextId() );
            thatFits.setInUse( true );
            result = true;

            if ( primitive.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                PropertyRecord first = getPropertyStore().getRecord(
                        primitive.getNextProp() );
                thatFits.setNextProp( first.getId() );
                first.setPrevProp( thatFits.getId() );
                getPropertyStore().updateRecord( first );
            }
            primitive.setNextProp( thatFits.getId() );
        }
        thatFits.addPropertyBlock( block );
        getPropertyStore().updateRecord( thatFits );
        return result;
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

    private int getPropertyKeyId( String name )
    {
        return propertyKeyTokens.idOf( name );
    }

    private long getOrCreateLabelId( String name )
    {
        long labelId = getLabelId( name );
        if ( labelId == -1 )
        {
            labelId = createNewLabelId( name );
        }
        return labelId;
    }

    private long getLabelId( String name )
    {
        return labelTokens.idOf( name );
    }

    private boolean primitiveHasProperty( PrimitiveRecord record,
                                          String propertyName )
    {
        long nextProp = record.getNextProp();
        int propertyKeyId = propertyKeyTokens.idOf( propertyName );
        if ( nextProp == Record.NO_NEXT_PROPERTY.intValue() || propertyKeyId == -1 )
        {
            return false;
        }

        PropertyRecord current;
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            current = getPropertyStore().getRecord( nextProp );
            if ( current.getPropertyBlock( propertyKeyId ) != null )
            {
                return true;
            }
            nextProp = current.getNextProp();
        }
        return false;
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
        NodeRecord nodeRecord = new NodeRecord( nodeId, Record.NO_NEXT_RELATIONSHIP.intValue(),
                                                Record.NO_NEXT_PROPERTY.intValue() );
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
        nodeRecord.setNextProp( createPropertyChain( properties ) );

        setNodeLabels( nodeRecord, labels );

        getNodeStore().updateRecord( nodeRecord );
        return nodeId;
    }

    private void setNodeLabels( NodeRecord nodeRecord, Label... labels )
    {
        NodeLabelRecordLogic manipulator = new NodeLabelRecordLogic( nodeRecord, getNodeStore() );
        Iterable<DynamicRecord> changedDynamicLabelRecords = manipulator.set( getOrCreateLabelIds( labels ) );
        getNodeStore().updateDynamicLabelRecords( changedDynamicLabelRecords );
    }

    private long[] getOrCreateLabelIds( Label[] labels )
    {
        long[] ids = new long[labels.length];
        for ( int i = 0; i < ids.length; i++ )
        {
            ids[i] = getOrCreateLabelId( labels[i].name() );
        }
        return ids;
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
        NodeRecord record = getNodeRecord( node );
        setNodeLabels( record, labels );
        getNodeStore().updateRecord( record );
    }

    @Override
    public Iterable<Label> getNodeLabels( long node )
    {
        NodeStore nodeStore = neoStore.getNodeStore();
        return map( labelIdToLabelFunction,
                    asIterable( getNodeStore().getLabelsForNode( nodeStore.getRecord( node ) ) ) );
    }

    @Override
    public boolean nodeHasLabel( long node, Label label )
    {
        long labelId = getLabelId( label.name() );
        return labelId != -1 && nodeHasLabel( node, labelId );
    }

    private boolean nodeHasLabel( long node, long labelId )
    {
        NodeStore nodeStore = neoStore.getNodeStore();
        long[] labels = getNodeStore().getLabelsForNode( nodeStore.getRecord( node ) );
        for ( long label : labels )
        {
            if ( label == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public long createRelationship( long node1, long node2, RelationshipType
            type, Map<String, Object> properties )
    {
        NodeRecord firstNode = getNodeRecord( node1 );
        NodeRecord secondNode = getNodeRecord( node2 );
        int typeId = relationshipTypeTokens.idOf( type.name() );
        if ( typeId == -1 )
        {
            typeId = createNewRelationshipType( type.name() );
        }
        long id = getRelationshipStore().nextId();
        RelationshipRecord record = new RelationshipRecord( id, node1, node2, typeId );
        record.setInUse( true );
        record.setCreated();
        connectRelationship( firstNode, secondNode, record );
        getNodeStore().updateRecord( firstNode );
        getNodeStore().updateRecord( secondNode );
        record.setNextProp( createPropertyChain( properties ) );
        getRelationshipStore().updateRecord( record );
        return id;
    }

    private void connectRelationship( NodeRecord firstNode,
                                      NodeRecord secondNode, RelationshipRecord rel )
    {
        assert firstNode.getNextRel() != rel.getId();
        assert secondNode.getNextRel() != rel.getId();
        rel.setFirstNextRel( firstNode.getNextRel() );
        rel.setSecondNextRel( secondNode.getNextRel() );
        connect( firstNode, rel );
        connect( secondNode, rel );
        firstNode.setNextRel( rel.getId() );
        secondNode.setNextRel( rel.getId() );
    }

    private void connect( NodeRecord node, RelationshipRecord rel )
    {
        if ( node.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = getRelationshipStore().getRecord( node.getNextRel() );
            boolean changed = false;
            if ( nextRel.getFirstNode() == node.getId() )
            {
                nextRel.setFirstPrevRel( rel.getId() );
                changed = true;
            }
            if ( nextRel.getSecondNode() == node.getId() )
            {
                nextRel.setSecondPrevRel( rel.getId() );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( node + " dont match " + nextRel );
            }
            getRelationshipStore().updateRecord( nextRel );
        }
    }

    @Override
    public void setNodeProperties( long node, Map<String, Object> properties )
    {
        NodeRecord record = getNodeRecord( node );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            deletePropertyChain( record.getNextProp() );
            /*
             * Batch inserter does not make any attempt to maintain the store's
             * integrity. It makes sense however to keep some things intact where
             * the cost is relatively low. So here, when we delete the property
             * chain we first make sure that the node record (or the relationship
             * record below) does not point anymore to the deleted properties. This
             * way, if during creation, something goes wrong, it will not have the properties
             * expected instead of throwing invalid record exceptions.
             */
            record.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
            getNodeStore().updateRecord( record );
        }
        record.setNextProp( createPropertyChain( properties ) );
        getNodeStore().updateRecord( record );
    }

    @Override
    public void setRelationshipProperties( long rel,
                                           Map<String, Object> properties )
    {
        RelationshipRecord record = getRelationshipRecord( rel );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            deletePropertyChain( record.getNextProp() );
            /*
             * See setNodeProperties above for an explanation of what goes on
             * here
             */
            record.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
            getRelationshipStore().updateRecord( record );
        }
        record.setNextProp( createPropertyChain( properties ) );
        getRelationshipStore().updateRecord( record );
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        return neoStore.getNodeStore().loadLightNode( nodeId ) != null;
    }

    @Override
    public Map<String, Object> getNodeProperties( long nodeId )
    {
        NodeRecord record = getNodeRecord( nodeId );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    @Override
    public Iterable<Long> getRelationshipIds( long nodeId )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        long nextRel = nodeRecord.getNextRel();
        List<Long> ids = new ArrayList<Long>();
        while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord relRecord = getRelationshipRecord( nextRel );
            ids.add( relRecord.getId() );
            long firstNode = relRecord.getFirstNode();
            long secondNode = relRecord.getSecondNode();
            if ( firstNode == nodeId )
            {
                nextRel = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                nextRel = relRecord.getSecondNextRel();
            }
            else
            {
                throw new InvalidRecordException( "Node[" + nodeId +
                                                  "] not part of firstNode[" + firstNode +
                                                  "] or secondNode[" + secondNode + "]" );
            }
        }
        return ids;
    }

    @Override
    public Iterable<BatchRelationship> getRelationships( long nodeId )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        long nextRel = nodeRecord.getNextRel();
        List<BatchRelationship> rels = new ArrayList<BatchRelationship>();
        while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord relRecord = getRelationshipRecord( nextRel );
            RelationshipType type = new RelationshipTypeImpl(
                    relationshipTypeTokens.nameOf( relRecord.getType() ) );
            rels.add( new BatchRelationship( relRecord.getId(),
                                             relRecord.getFirstNode(), relRecord.getSecondNode(), type ) );
            long firstNode = relRecord.getFirstNode();
            long secondNode = relRecord.getSecondNode();
            if ( firstNode == nodeId )
            {
                nextRel = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                nextRel = relRecord.getSecondNextRel();
            }
            else
            {
                throw new InvalidRecordException( "Node[" + nodeId +
                                                  "] not part of firstNode[" + firstNode +
                                                  "] or secondNode[" + secondNode + "]" );
            }
        }
        return rels;
    }

    @Override
    public BatchRelationship getRelationshipById( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId );
        RelationshipType type = new RelationshipTypeImpl(
                relationshipTypeTokens.nameOf( record.getType() ) );
        return new BatchRelationship( record.getId(), record.getFirstNode(),
                                      record.getSecondNode(), type );
    }

    @Override
    public Map<String, Object> getRelationshipProperties( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    @Override
    public void shutdown()
    {
        if ( isShutdown )
        {
            throw new IllegalStateException( "Batch inserter already has shutdown" );
        }
        isShutdown = true;

        try
        {
            repopulateAllIndexes();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        neoStore.close();
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

    private long createPropertyChain( Map<String, Object> properties )
    {
        if ( properties == null || properties.isEmpty() )
        {
            return Record.NO_NEXT_PROPERTY.intValue();
        }
        PropertyStore propStore = getPropertyStore();
        List<PropertyRecord> propRecords = new ArrayList<PropertyRecord>();
        PropertyRecord currentRecord = new PropertyRecord( propStore.nextId() );
        currentRecord.setInUse( true );
        currentRecord.setCreated();
        propRecords.add( currentRecord );
        for ( Entry<String, Object> entry : properties.entrySet() )
        {
            int keyId = propertyKeyTokens.idOf( entry.getKey() );
            if ( keyId == -1 )
            {
                keyId = createNewPropertyKeyId( entry.getKey() );
            }

            PropertyBlock block = new PropertyBlock();
            propStore.encodeValue( block, keyId, entry.getValue() );
            if ( currentRecord.size() + block.getSize() > PropertyType.getPayloadSize() )
            {
                // Here it means the current block is done for
                PropertyRecord prevRecord = currentRecord;
                // Create new record
                long propertyId = propStore.nextId();
                currentRecord = new PropertyRecord( propertyId );
                currentRecord.setInUse( true );
                currentRecord.setCreated();
                // Set up links
                prevRecord.setNextProp( propertyId );
                currentRecord.setPrevProp( prevRecord.getId() );
                propRecords.add( currentRecord );
                // Now current is ready to start picking up blocks
            }
            currentRecord.addPropertyBlock( block );
        }
        /*
         * Add the property records in reverse order, which means largest
         * id first. That is to make sure we expand the property store file
         * only once.
         */
        for ( int i = propRecords.size() - 1; i >= 0; i-- )
        {
            propStore.updateRecord( propRecords.get( i ) );
        }
        /*
         *  0 will always exist, if the map was empty we wouldn't be here
         *  and even one property will create at least one record.
         */
        return propRecords.get( 0 ).getId();
    }

    private void deletePropertyChain( long nextProp )
    {
        PropertyStore propStore = getPropertyStore();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propStore.getRecord( nextProp );
            /*
             *  The only reason to loop over the blocks is to handle the dynamic
             *  records that possibly hang under them. Otherwise, we could just
             *  set the property record not in use and be done with it. The
             *  residue of the convenience is that we do not remove individual
             *  property blocks - we just mark the whole record as !inUse.
             */
            for ( PropertyBlock propBlock : propRecord.getPropertyBlocks() )
            {
                propStore.ensureHeavy( propBlock );
                for ( DynamicRecord rec : propBlock.getValueRecords() )
                {
                    rec.setInUse( false );
                    propRecord.addDeletedRecord( rec );
                }
            }
            propRecord.setInUse( false );
            nextProp = propRecord.getNextProp();
            propStore.updateRecord( propRecord );
        }
    }

    private Map<String, Object> getPropertyChain( long nextProp )
    {
        PropertyStore propStore = getPropertyStore();
        Map<String, Object> properties = new HashMap<String, Object>();

        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propStore.getRecord( nextProp );
            for ( PropertyBlock propBlock : propRecord.getPropertyBlocks() )
            {
                String key = propertyKeyTokens.nameOf( propBlock.getKeyIndexId() );
                PropertyData propertyData = propBlock.newPropertyData( propRecord );
                Object value = propertyData.getValue() != null ? propertyData.getValue() :
                               propBlock.getType().getValue( propBlock, getPropertyStore() );
                properties.put( key, value );
            }
            nextProp = propRecord.getNextProp();
        }
        return properties;
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

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    private PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return getPropertyStore().getPropertyKeyTokenStore();
    }

    private RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    private RelationshipTypeTokenStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    private SchemaStore getSchemaStore()
    {
        return neoStore.getSchemaStore();
    }

    private NodeRecord getNodeRecord( long id )
    {
        if ( id < 0 || id >= getNodeStore().getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return getNodeStore().getRecord( id );
    }

    private RelationshipRecord getRelationshipRecord( long id )
    {
        if ( id < 0 || id >= getRelationshipStore().getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return getRelationshipStore().getRecord( id );
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

    @Override
    public long getReferenceNode()
    {
        if ( nodeExists( 0 ) )
        {
            return 0;
        }
        return -1;
    }

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
            createIndexRule( label, propertyKey );
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
            long labelId = getOrCreateLabelId( label.name() );
            int propertyKeyId = getOrCreatePropertyKeyId( propertyKey );
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

        private UnsupportedOperationException unsupportedException()
        {
            return new UnsupportedOperationException( "Batch inserter doesn't support this" );
        }
    }

    private class DependencyResolverImpl extends DependencyResolver.Adapter
    {
        @Override
        public <T> T resolveDependency( Class<T> type, SelectionStrategy<T> selector ) throws IllegalArgumentException
        {
            if ( type.isInstance( fileSystem ) )
            {
                return type.cast( fileSystem );
            }
            if ( type.isInstance( config ) )
            {
                return type.cast( config );
            }
            throw new IllegalArgumentException( "Unknown dependency " + type );
        }
    }
}
