/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.old;

import static org.neo4j.consistency.checking.old.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType
        .DYNAMIC_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType
        .ILLEGAL_PROPERTY_TYPE;
import static org.neo4j.consistency.checking.old.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType
        .INVALID_PROPERTY_KEY;
import static org.neo4j.consistency.checking.old.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType
        .UNUSED_PROPERTY_KEY;
import static org.neo4j.consistency.checking.old.InconsistencyType.PropertyOwnerInconsistency.OwnerInconsistencyType
        .MULTIPLE_OWNERS;
import static org.neo4j.consistency.checking.old.InconsistencyType.PropertyOwnerInconsistency.OwnerInconsistencyType
        .PROPERTY_CHANGED_FOR_WRONG_OWNER;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.DYNAMIC_LENGTH_TOO_LARGE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.INVALID_TYPE_ID;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.NEXT_DYNAMIC_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.NEXT_DYNAMIC_NOT_REMOVED;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.NEXT_PROPERTY_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.NON_FULL_DYNAMIC_WITH_NEXT;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.ORPHANED_PROPERTY;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.OVERWRITE_USED_DYNAMIC;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.OWNER_DOES_NOT_REFERENCE_BACK;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.OWNER_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.PREV_PROPERTY_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .PROPERTY_CHANGED_WITHOUT_OWNER;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.PROPERTY_FOR_OTHER;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .PROPERTY_NEXT_WRONG_BACKREFERENCE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.PROPERTY_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .PROPERTY_PREV_WRONG_BACKREFERENCE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.RELATIONSHIP_FOR_OTHER_NODE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.RELATIONSHIP_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .RELATIONSHIP_NOT_REMOVED_FOR_DELETED_NODE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .REMOVED_PROPERTY_STILL_REFERENCED;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency
        .REMOVED_RELATIONSHIP_STILL_REFERENCED;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.REPLACED_PROPERTY;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.TYPE_NOT_IN_USE;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.UNUSED_KEY_NAME;
import static org.neo4j.consistency.checking.old.InconsistencyType.ReferenceInconsistency.UNUSED_TYPE_NAME;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.neo4j.consistency.repair.RelationshipChainField;
import org.neo4j.consistency.repair.RelationshipNodeField;
import org.neo4j.consistency.store.DiffRecordStore;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;

@Deprecated
public class ConsistencyRecordProcessor extends RecordStore.Processor<RuntimeException> implements Runnable
{
    private final RecordStore<NodeRecord> nodes;
    private final RecordStore<RelationshipRecord> rels;
    private final RecordStore<PropertyRecord> props;
    private final RecordStore<DynamicRecord> strings, arrays;
    private final RecordStore<PropertyKeyTokenRecord>  propIndexes;
    private final RecordStore<RelationshipTypeTokenRecord>  relTypes;
    private final RecordStore<DynamicRecord> propKeys;
    private final RecordStore<DynamicRecord> typeNames;

    private final HashMap<Long/*property record id*/, PropertyOwner> propertyOwners;
    private long brokenNodes, brokenRels, brokenProps, brokenStrings, brokenArrays, brokenTypes, brokenKeys;
    private final InconsistencyReport report;

    private final static RelationshipNodeField[] nodeFields = RelationshipNodeField.values();
    private final static RelationshipChainField[] relFields = RelationshipChainField.values();
    private final ProgressMonitorFactory progressFactory;

    public ConsistencyRecordProcessor( StoreAccess stores, InconsistencyReport report)
    {
        this( stores, false, report, ProgressMonitorFactory.NONE );
    }

    /**
     * Creates a standard checker.
     *
     * @param stores the stores to check.
     */
    public ConsistencyRecordProcessor( StoreAccess stores, InconsistencyReport report, ProgressMonitorFactory progressFactory )
    {
        this( stores, false, report, progressFactory );
    }

    /**
     * Creates a standard checker or a checker that validates property owners.
     *
     * Property ownership validation validates that each property record is only
     * referenced once. This check has a bit of memory overhead.
     *
     * @param stores the stores to check.
     * @param checkPropertyOwners if <code>true</code> ownership validation will
     */
    public ConsistencyRecordProcessor( StoreAccess stores, boolean checkPropertyOwners, InconsistencyReport report,
                                       ProgressMonitorFactory progressFactory )
    {
        this.nodes = stores.getNodeStore();
        this.rels = stores.getRelationshipStore();
        this.props = stores.getPropertyStore();
        this.strings = stores.getStringStore();
        this.arrays = stores.getArrayStore();
        this.relTypes = stores.getRelationshipTypeTokenStore();
        this.propIndexes = stores.getPropertyKeyTokenStore();
        this.propKeys = stores.getPropertyKeyNameStore();
        this.typeNames = stores.getRelationshipTypeNameStore();
        this.propertyOwners = checkPropertyOwners ? new HashMap<Long, PropertyOwner>() : null;
        this.report = report;
        this.progressFactory = progressFactory;
    }

    @Override
    public void processNode( RecordStore<NodeRecord> store, NodeRecord node )
    {
        if ( checkNode( node ) ) brokenNodes++;
    }

    @Override
    public void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
    {
        if ( checkRelationship( rel ) ) brokenRels++;
    }

    @Override
    public void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
    {
        if ( checkProperty( property ) ) brokenProps++;
    }

    @Override
    public void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType )
    {
        if ( checkDynamic( store, string ) )
        {
            brokenStrings++;
        }
    }

    @Override
    public void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        if ( checkDynamic( store, array ) ) brokenArrays++;
    }

    @Override
    public void processRelationshipType( RecordStore<RelationshipTypeTokenRecord> store, RelationshipTypeTokenRecord type )
    {
        if ( checkType( type ) ) brokenTypes++;
    }

    @Override
    public void processPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord index )
    {
        if ( checkKey( index ) ) brokenKeys++;
    }

    private boolean checkNode( NodeRecord node )
    {
        boolean fail = false;
        if ( !node.inUse() )
        {
            NodeRecord old = nodes.forceGetRaw( node );
            if ( old.inUse() ) // Check that referenced records are also removed
            {
                if ( !Record.NO_NEXT_RELATIONSHIP.is( old.getNextRel() ) )
                { // NOTE: with reuse in the same tx this check is invalid
                    RelationshipRecord rel = rels.forceGetRecord( old.getNextRel() );
                    if ( rel.inUse() ) fail |= report.inconsistent( nodes, node, rels, rel, RELATIONSHIP_NOT_REMOVED_FOR_DELETED_NODE );
                }
                fail |= checkPropertyReference( node, nodes, new PropertyOwner.OwningNode( node.getId() ) );
            }
            return fail;
        }
        long relId = node.getNextRel();
        if ( !Record.NO_NEXT_RELATIONSHIP.is( relId ) )
        {
            RelationshipRecord rel = rels.forceGetRecord( relId );
            if ( !rel.inUse() )
                fail |= report.inconsistent( nodes, node, rels, rel, RELATIONSHIP_NOT_IN_USE );
            else if ( !( rel.getFirstNode() == node.getId() || rel.getSecondNode() == node.getId() ) )
                fail |= report.inconsistent( nodes, node, rels, rel, RELATIONSHIP_FOR_OTHER_NODE );
        }
        fail |= checkPropertyReference( node, nodes, new PropertyOwner.OwningNode( node.getId() ) );
        return fail;
    }

    private <R extends PrimitiveRecord> boolean checkPropertyReference( R primitive, RecordStore<R> store, PropertyOwner owner )
    {
        boolean fail = false;
        if ( props != null )
        {
            R old = store.forceGetRaw( primitive );
            if ( primitive.inUse() )
            {
                if ( !Record.NO_NEXT_PROPERTY.is( primitive.getNextProp() ) )
                {
                    PropertyRecord prop = props.forceGetRecord( primitive.getNextProp() );
                    fail |= checkPropertyOwner( prop, owner );
                    if ( !prop.inUse() )
                        fail |= report.inconsistent( store, primitive, props, prop, PROPERTY_NOT_IN_USE );
                    else if ( owner.otherOwnerOf( prop ) != -1
                            || ( owner.ownerOf( prop ) != -1 && owner.ownerOf( prop ) != primitive.getId() ) )
                        fail |= report.inconsistent( store, primitive, props, prop, PROPERTY_FOR_OTHER );
                }
                if ( old.inUse() && old.getNextProp() != primitive.getNextProp() )
                { // first property changed for this primitive record ...
                    if ( !Record.NO_NEXT_PROPERTY.is( old.getNextProp() ) )
                    {
                        PropertyRecord oldProp = props.forceGetRecord( old.getNextProp() );
                        if ( owner.ownerOf( oldProp ) != primitive.getId() )
                            // ... but the old first property record didn't change accordingly
                            fail |= report.inconsistent( props, oldProp, store, primitive, ORPHANED_PROPERTY );
                    }
                }
            }
            else
            {
                if ( !Record.NO_NEXT_PROPERTY.is( old.getNextProp() ) )
                { // NOTE: with reuse in the same tx this check is invalid
                    PropertyRecord prop = props.forceGetRecord( old.getNextProp() );
                    if ( prop.inUse() )
                        fail |= report.inconsistent( store, primitive, props, prop, owner.propertyNotRemoved() );
                }
            }
        }
        return fail;
    }

    private boolean checkRelationship( RelationshipRecord rel )
    {
        boolean fail = false;
        if ( !rel.inUse() )
        {
            RelationshipRecord old = rels.forceGetRaw( rel );
            if ( old.inUse() )
            {
                for (RelationshipChainField field : relFields)
                {
                    long otherId = field.relOf( old );
                    if (otherId == field.none)
                    {
                        Long nodeId = field.nodeOf( old );
                        if (nodeId != null)
                        {
                            NodeRecord node = nodes.forceGetRecord( nodeId );
                            if (node.inUse() && node.getNextRel() == old.getId())
                                fail |= report.inconsistent( rels, rel, nodes, node, REMOVED_RELATIONSHIP_STILL_REFERENCED );
                        }
                    }
                    else
                    {
                        RelationshipRecord other = rels.forceGetRecord( otherId );
                        if (other.inUse() && field.invConsistent( old, other ))
                            fail |= report.inconsistent( rels,rel, other, REMOVED_RELATIONSHIP_STILL_REFERENCED );
                    }
                }
                fail |= checkPropertyReference( rel, rels, new PropertyOwner.OwningRelationship( rel.getId() ) );
            }
            return fail;
        }
        if ( rel.getType() < 0 ) fail |= report.inconsistent( rels, rel, INVALID_TYPE_ID );
        else
        {
            RelationshipTypeTokenRecord type = relTypes.forceGetRecord( rel.getType() );
            if ( !type.inUse() ) fail |= report.inconsistent( rels, rel, relTypes, type, TYPE_NOT_IN_USE );
        }
        for ( RelationshipChainField field : relFields )
        {
            long otherId = field.relOf( rel );
            if ( otherId == field.none )
            {
                Long nodeId = field.nodeOf( rel );
                if ( nodeId != null )
                {
                    NodeRecord node = nodes.forceGetRecord( nodeId );
                    if ( !node.inUse() || node.getNextRel() != rel.getId() )
                        fail |= report.inconsistent( rels, rel, nodes, node, field.noBackReference );
                }
            }
            else
            {
                RelationshipRecord other = rels.forceGetRecord( otherId );
                if ( !other.inUse() )
                    fail |= report.inconsistent( rels, rel, other, field.notInUse );
                else if ( !field.invConsistent( rel, other ) )
                    fail |= report.inconsistent( rels, rel, other, field.differentChain );
            }
        }
        for ( RelationshipNodeField field : nodeFields )
        {
            long nodeId = field.get( rel );
            if ( nodeId < 0 )
                fail |= report.inconsistent( rels, rel, field.invalidReference);
            else
            {
                NodeRecord node = nodes.forceGetRecord( nodeId );
                if ( !node.inUse() )
                    fail |= report.inconsistent( rels, rel, nodes, node, field.notInUse );
            }
        }
        fail |= checkPropertyReference( rel, rels, new PropertyOwner.OwningRelationship( rel.getId() ) );
        return fail;
    }

    private boolean checkPropertyOwner( PropertyRecord prop, PropertyOwner newOwner )
    {
        if (propertyOwners == null) return false;
        Long propId = Long.valueOf( prop.getId() );
        PropertyOwner oldOwner = propertyOwners.put( propId, newOwner );
        if ( oldOwner != null )
        {
            @SuppressWarnings( "unchecked" )
            RecordStore<PrimitiveRecord> oldStore = (RecordStore<PrimitiveRecord>) oldOwner.storeFrom( nodes, rels ),
                    newStore = (RecordStore<PrimitiveRecord>) newOwner.storeFrom( nodes, rels  );
            return report.inconsistent( oldStore, oldStore.getRecord( oldOwner.id ),
                    newStore, newStore.getRecord( newOwner.id ),
                    MULTIPLE_OWNERS.forProperty( prop ) );
        }
        else
        {
            return false;
        }
    }

    private boolean checkProperty( PropertyRecord property )
    {
        boolean fail = false;
        if ( !property.inUse() )
        {
            PropertyRecord old = props.forceGetRaw( property );
            if ( old.inUse() )
            {
                if ( !Record.NO_NEXT_PROPERTY.is( old.getNextProp() ) )
                {
                    PropertyRecord next = props.forceGetRecord( old.getNextProp() );
                    if ( next.inUse() && next.getPrevProp() == old.getId() )
                        fail |= report.inconsistent( props, property, next, REMOVED_PROPERTY_STILL_REFERENCED );
                }
                if ( !Record.NO_PREVIOUS_PROPERTY.is( old.getPrevProp() ) )
                {
                    PropertyRecord prev = props.forceGetRecord( old.getPrevProp() );
                    if ( prev.inUse() && prev.getNextProp() == old.getId() )
                        fail |= report.inconsistent( props, property, prev, REMOVED_PROPERTY_STILL_REFERENCED );
                }
                else // property was first in chain
                {
                    if ( property.getNodeId() != -1 )
                        fail |= checkPropertyOwnerReference( property, property.getNodeId(), nodes );
                    else if ( property.getRelId() != -1 )
                        fail |= checkPropertyOwnerReference( property, property.getRelId(), rels );
                    else if ( ((DiffRecordStore<PropertyRecord>)props).isModified( property.getId() ) )
                        fail |= report.inconsistent( props, property, PROPERTY_CHANGED_WITHOUT_OWNER ); // only a warning
                }
                fail |= checkOwnerChain( property );
            }
            return fail;
        }
        long nextId = property.getNextProp();
        if ( !Record.NO_NEXT_PROPERTY.is( nextId ) )
        {
            PropertyRecord next = props.forceGetRecord( nextId );
            if ( !next.inUse() )
                fail |= report.inconsistent( props, property, next, NEXT_PROPERTY_NOT_IN_USE );
            if ( next.getPrevProp() != property.getId() )
                fail |= report.inconsistent( props, property, next, PROPERTY_NEXT_WRONG_BACKREFERENCE );
        }
        long prevId = property.getPrevProp();
        if ( !Record.NO_PREVIOUS_PROPERTY.is( prevId ) )
        {
            PropertyRecord prev = props.forceGetRecord( prevId );
            if ( !prev.inUse() )
                fail |= report.inconsistent( props, property, prev, PREV_PROPERTY_NOT_IN_USE );
            if ( prev.getNextProp() != property.getId() )
                fail |= report.inconsistent( props, property, prev, PROPERTY_PREV_WRONG_BACKREFERENCE );
        }
        else // property is first in chain
        {
            if ( property.getNodeId() != -1 )
                fail |= checkPropertyOwnerReference( property, property.getNodeId(), nodes );
            else if ( property.getRelId() != -1 )
                fail |= checkPropertyOwnerReference( property, property.getRelId(), rels );
            else if ( props instanceof DiffRecordStore<?>
                    && ( (DiffRecordStore<PropertyRecord>) props ).isModified( property.getId() ) )
                fail |= report.inconsistent( props, property, PROPERTY_CHANGED_WITHOUT_OWNER ); // only a warning
            // else - this information is only available from logs through DiffRecordStore
        }
        fail |= checkOwnerChain( property );
        for ( PropertyBlock block : property.getPropertyBlocks() )
        {
            if ( block.getKeyIndexId() < 0 ) fail |= report.inconsistent( props, property, INVALID_PROPERTY_KEY.forBlock( block ) );
            else
            {
                PropertyKeyTokenRecord key = propIndexes.forceGetRecord( block.getKeyIndexId() );
                if ( !key.inUse() ) fail |= report.inconsistent( props, property, propIndexes, key, UNUSED_PROPERTY_KEY.forBlock(  block ) );
            }
            RecordStore<DynamicRecord> dynStore = null;
            PropertyType type = block.forceGetType();
            if ( type == null )
            {
                fail |= report.inconsistent( props, property, ILLEGAL_PROPERTY_TYPE.forBlock( block ) );
            }
            else switch ( block.getType() )
            {
                case STRING:
                    dynStore = strings;
                    break;
                case ARRAY:
                    dynStore = arrays;
                    break;
            }
            if ( dynStore != null )
            {
                DynamicRecord dynrec = dynStore.forceGetRecord( block.getSingleValueLong() );
                if ( !dynrec.inUse() )
                    fail |= report.inconsistent( props, property, dynStore, dynrec, DYNAMIC_NOT_IN_USE.forBlock( block ) );
            }
        }
        return fail;
    }

    private boolean checkOwnerChain( PropertyRecord property )
    {
        boolean fail = false;
        RecordStore<? extends PrimitiveRecord> store = null;
        long ownerId = -1;
        if ( property.getNodeId() != -1 )
        {
            store = nodes;
            ownerId = property.getNodeId();
        }
        else if ( property.getRelId() != -1 )
        {
            store = rels;
            ownerId = property.getRelId();
        }
        if ( store != null )
        {
            PrimitiveRecord owner = store.forceGetRecord( ownerId );
            if ( !property.inUse() )
            {
                owner = ((RecordStore<PrimitiveRecord>) store).forceGetRaw( owner );
            }
            List<PropertyRecord> chain = new ArrayList<PropertyRecord>( 2 );
            PropertyRecord prop = null;
            for ( long propId = owner.getNextProp(), target = property.getId(); propId != target; propId = prop.getNextProp() )
            {
                if ( Record.NO_NEXT_PROPERTY.is( propId ) )
                {
                    fail |= report.inconsistent( props, property, store, owner, PROPERTY_CHANGED_FOR_WRONG_OWNER.forProperties( chain ) );
                    break; // chain ended, not found
                }
                prop = props.forceGetRecord( propId );
                if ( !property.inUse() )
                {
                    prop = props.forceGetRaw( prop );
                }
                chain.add( prop );
            }
        }
        else if ( props instanceof DiffRecordStore<?> && ( (DiffRecordStore<?>) props ).isModified( property.getId() ) )
            fail |= report.inconsistent( props, property, PROPERTY_CHANGED_WITHOUT_OWNER ); // only a warning
        return fail;
    }

    private boolean checkPropertyOwnerReference( PropertyRecord property, long ownerId, RecordStore<? extends PrimitiveRecord> entityStore )
    {
        boolean fail = false;
        PrimitiveRecord entity = entityStore.forceGetRecord( ownerId );
        if ( !property.inUse() )
        {
            if ( entity.inUse() )
            {
                if ( entity.getNextProp() == property.getId() )
                    fail |= report.inconsistent( props, property, entityStore, entity, REMOVED_PROPERTY_STILL_REFERENCED );
            }
            return fail;
        }
        if ( !entity.inUse() )
            fail |= report.inconsistent( props, property, entityStore, entity, OWNER_NOT_IN_USE );
        else if ( entity.getNextProp() != property.getId() )
            fail |= report.inconsistent( props, property, entityStore, entity, OWNER_DOES_NOT_REFERENCE_BACK );
        if ( entityStore instanceof DiffRecordStore<?> )
        {
            DiffRecordStore<PrimitiveRecord> diffs = (DiffRecordStore<PrimitiveRecord>) entityStore;
            if ( diffs.isModified( entity.getId() ) )
            {
                PrimitiveRecord old = diffs.forceGetRaw( entity );
                // IF old is in use and references a property record
                if ( old.inUse() && !Record.NO_NEXT_PROPERTY.is( old.getNextProp() ) )
                    // AND that property record is not the same as this property record
                    if ( old.getNextProp() != property.getId() )
                        // THEN that property record must also have been updated!
                        if ( !( (DiffRecordStore<?>) props ).isModified( old.getNextProp() ) )
                            fail |= report.inconsistent( props, property, entityStore, entity, REPLACED_PROPERTY );
            }
        }
        return fail;
    }

    private boolean checkDynamic( RecordStore<DynamicRecord> store, DynamicRecord record )
    {
        boolean fail = false;
        if ( !record.inUse() )
        {
            if ( store instanceof DiffRecordStore<?> )
            {
                DynamicRecord old = store.forceGetRaw( record );
                if ( old.inUse() && !Record.NO_NEXT_BLOCK.is( old.getNextBlock() ) )
                {
                    DynamicRecord next = store.forceGetRecord( old.getNextBlock() );
                    if ( next.inUse() ) // the entire chain must be removed
                        fail |= report.inconsistent( store, record, next, NEXT_DYNAMIC_NOT_REMOVED );
                }
            }
            return fail;
        }
        long nextId = record.getNextBlock();
        if ( !Record.NO_NEXT_BLOCK.is( nextId ) )
        {
            // If next is set, then it must be in use
            DynamicRecord next = store.forceGetRecord( nextId );
            if ( !next.inUse() )
                fail |= report.inconsistent( store, record, next, NEXT_DYNAMIC_NOT_IN_USE );
            // If next is set, then the size must be max
            if ( record.getLength() < store.getRecordSize() - store.getRecordHeaderSize() )
                fail |= report.inconsistent( store, record, NON_FULL_DYNAMIC_WITH_NEXT );
        }
        if ( record.getId() != 0
                && record.getLength() > store.getRecordSize()
                - store.getRecordHeaderSize() )
        {
            /*
             *  The length must always be less than or equal to max,
             *  except for the first dynamic record in a store, which
             *  does not conform to the usual format
             */
            fail |= report.inconsistent( store, record, DYNAMIC_LENGTH_TOO_LARGE );
        }
        if ( store instanceof DiffRecordStore<?> )
        {
            DiffRecordStore<DynamicRecord> diffs = (DiffRecordStore<DynamicRecord>) store;
            if ( diffs.isModified( record.getId() ) && !record.isLight() )
            { // if the record is really modified it will be heavy
                DynamicRecord prev = diffs.forceGetRaw( record );
                if ( prev.inUse() ) fail |= report.inconsistent( store, record, prev, OVERWRITE_USED_DYNAMIC );
            }
        }
        return fail;
    }

    private boolean checkType( RelationshipTypeTokenRecord type )
    {
        if ( !type.inUse() ) return false; // no check for unused records
        if ( Record.NO_NEXT_BLOCK.is( type.getNameId() ) ) return false; // accept this
        DynamicRecord record = typeNames.forceGetRecord( type.getNameId() );
        if ( !record.inUse() ) return report.inconsistent( relTypes, type, typeNames, record, UNUSED_TYPE_NAME );
        return false;
    }

    private boolean checkKey( PropertyKeyTokenRecord key )
    {
        if ( !key.inUse() ) return false; // no check for unused records
        if ( Record.NO_NEXT_BLOCK.is( key.getNameId() ) ) return false; // accept this
        DynamicRecord record = propKeys.forceGetRecord( key.getNameId() );
        if ( !record.inUse() ) return report.inconsistent( propIndexes, key, propKeys, record, UNUSED_KEY_NAME );
        return false;
    }

    @Override
    public void run()
    {
        ProgressMonitorFactory.MultiPartBuilder builder = progressFactory.multipleParts( "ConsistencyCheck" );

        List<Runnable> tasks = new ArrayList<Runnable>( 9 );

        tasks.add( storeProcessor( nodes, builder ) );
        tasks.add( storeProcessor( rels, builder ) );
        // free up some heap space that isn't needed anymore
        if ( propertyOwners != null ) propertyOwners.clear(); // TODO: invoke in proper order
        tasks.add( storeProcessor( props, builder ) );
        // free up some heap space that isn't needed anymore
        if ( propertyOwners != null ) propertyOwners.clear(); // TODO: invoke in proper order

        tasks.add( storeProcessor( strings, builder ) );
        tasks.add( storeProcessor( arrays, builder ) );
        tasks.add( storeProcessor( relTypes, builder ) );
        tasks.add( storeProcessor( propIndexes, builder ) );
        tasks.add( storeProcessor( propKeys, builder ) );
        tasks.add( storeProcessor( typeNames, builder ) );

        builder.build();

        for ( Runnable task : tasks )
        {
            task.run();
        }
    }

    private <R extends AbstractBaseRecord> StoreProcessor<R> storeProcessor( RecordStore<R> store,
                                                                             ProgressMonitorFactory.MultiPartBuilder builder )
    {
        return new StoreProcessor<R>( store, builder );
    }

    private class StoreProcessor<R extends AbstractBaseRecord> implements Runnable
    {
        private final RecordStore<R> store;
        private final ProgressListener progressListener;

        private StoreProcessor( RecordStore<R> store, ProgressMonitorFactory.MultiPartBuilder builder )
        {
            this.store = store;
            File name = store.getStorageFileName();
            this.progressListener = builder.progressForPart( name.getName(), store.getHighId() );
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public void run()
        {
            applyFiltered( store, progressListener, RecordStore.IN_USE );
        }
    }
}
