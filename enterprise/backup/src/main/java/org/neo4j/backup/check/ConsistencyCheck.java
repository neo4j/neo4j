/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.backup.check;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.ProgressIndicator;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;

import static org.neo4j.backup.check.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType.DYNAMIC_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType.ILLEGAL_PROPERTY_TYPE;
import static org.neo4j.backup.check.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType.INVALID_PROPERTY_KEY;
import static org.neo4j.backup.check.InconsistencyType.PropertyBlockInconsistency.BlockInconsistencyType.UNUSED_PROPERTY_KEY;
import static org.neo4j.backup.check.InconsistencyType.PropertyOwnerInconsistency.OwnerInconsistencyType.MULTIPLE_OWNERS;
import static org.neo4j.backup.check.InconsistencyType.PropertyOwnerInconsistency.OwnerInconsistencyType.PROPERTY_CHANGED_FOR_WRONG_OWNER;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.DYNAMIC_LENGTH_TOO_LARGE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.INVALID_TYPE_ID;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.NEXT_DYNAMIC_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.NEXT_DYNAMIC_NOT_REMOVED;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.NEXT_PROPERTY_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.NON_FULL_DYNAMIC_WITH_NEXT;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.OVERWRITE_USED_DYNAMIC;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.OWNER_DOES_NOT_REFERENCE_BACK;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.OWNER_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PREV_PROPERTY_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PROPERTY_CHANGED_WITHOUT_OWNER;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PROPERTY_FOR_OTHER;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PROPERTY_NEXT_WRONG_BACKREFERENCE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PROPERTY_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PROPERTY_NOT_REMOVED_FOR_DELETED_NODE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.PROPERTY_PREV_WRONG_BACKREFERENCE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.RELATIONSHIP_FOR_OTHER_NODE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.RELATIONSHIP_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.RELATIONSHIP_NOT_REMOVED_FOR_DELETED_NODE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.REMOVED_PROPERTY_STILL_REFERENCED;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.REMOVED_RELATIONSHIP_STILL_REFERENCED;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.REPLACED_PROPERTY;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.SOURCE_NEXT_DIFFERENT_CHAIN;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.SOURCE_NEXT_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.SOURCE_NODE_INVALID;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.SOURCE_NODE_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.SOURCE_NO_BACKREF;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.SOURCE_PREV_DIFFERENT_CHAIN;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.SOURCE_PREV_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TARGET_NEXT_DIFFERENT_CHAIN;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TARGET_NEXT_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TARGET_NODE_INVALID;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TARGET_NODE_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TARGET_NO_BACKREF;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TARGET_PREV_DIFFERENT_CHAIN;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TARGET_PREV_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.TYPE_NOT_IN_USE;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.UNUSED_KEY_NAME;
import static org.neo4j.backup.check.InconsistencyType.ReferenceInconsistency.UNUSED_TYPE_NAME;

/**
 * Finds inconsistency in a Neo4j store.
 *
 * Warning: will not find "dangling" records, i.e. records that are correct but
 * not referenced.
 *
 * Warning: will only find multiple references to the same property chain or
 * dynamic record chain for incremental checks (if the {@link RecordStore stores}
 * are {@link DiffRecordStore diff stores}). Also, this checking is very
 * incomplete.
 */
public abstract class ConsistencyCheck extends RecordStore.Processor implements Runnable, Iterable<RecordStore<?>>
{
    /**
     * Run a full consistency check on the specified store.
     *
     * @param args The arguments to the checker, the first is taken as the path
     *            to the store to check.
     */
    public static void main( String... args )
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = new Args( args );
        boolean propowner = params.getBoolean( "propowner", false, true );
        args = params.orphans().toArray( new String[0] );
        if ( args.length != 1 )
        {
            printUsage();
            System.exit( -1 );
            return;
        }
        StoreAccess stores = new StoreAccess( args[0] );
        try
        {
            run( stores, propowner );
        }
        finally
        {
            stores.close();
        }
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines ) System.err.println( line );
        System.err.println( Args.jarUsage( ConsistencyCheck.class, "[-propowner] <storedir>" ) );
        System.err.println( "WHERE:   <storedir>  is the path to the store to check" );
        System.err.println( "         -propowner  --  to verify that properties are owned only once" );
    }

    public static void run( StoreAccess stores, boolean propowner )
    {
        new ConsistencyCheck( stores, propowner )
        {
            @Override
            ProgressIndicator.MultiProgress progressInit()
            {
                System.err.println( "Checking consistency on:" );
                long total = 0;
                for ( RecordStore<?> store : this )
                {
                    if ( store != null )
                    {
                        long highId = store.getHighId();
                        System.err.println( "    " + highId + " records from " + store );
                        total += highId;
                    }
                }
                return ProgressIndicator.MultiProgress.textual( System.err, total );
            }

            @Override
            protected <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                    RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
                    InconsistencyType inconsistency )
            {
                System.out.println( record + " " + referred + " //" + inconsistency.message() );
            }

            @Override
            protected <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record,
                    InconsistencyType inconsistency )
            {
                System.out.println( record + " //" + inconsistency.message() );
            }
        }.run();
    }

    private final RecordStore<NodeRecord> nodes;
    private final RecordStore<RelationshipRecord> rels;
    private final RecordStore<PropertyRecord> props;
    private final RecordStore<DynamicRecord> strings, arrays;
    private final RecordStore<PropertyIndexRecord>  propIndexes;
    private final RecordStore<RelationshipTypeRecord>  relTypes;
    private final RecordStore<DynamicRecord> propKeys;
    private final RecordStore<DynamicRecord> typeNames;
    @Override
    public Iterator<RecordStore<?>> iterator()
    {
        return Arrays.<RecordStore<?>>asList( nodes, rels, props, strings, arrays, propIndexes, relTypes, propKeys,
                typeNames ).iterator();
    }
    private final HashMap<Long/*property record id*/, PropertyOwner> propertyOwners;
    private long brokenNodes, brokenRels, brokenProps, brokenStrings, brokenArrays, brokenTypes, brokenKeys;

    /**
     * Creates a standard checker.
     *
     * @param stores the stores to check.
     */
    public ConsistencyCheck( StoreAccess stores )
    {
        this( stores, false );
    }

    /**
     * Creates a standard checker or a checker that validates property owners.
     *
     * Property ownership validation validates that each property record is only
     * referenced once. This check has a bit of memory overhead.
     *
     * @param stores the stores to check.
     * @param checkPropertyOwners if <code>true</code> ownership validation will
     *            be performed.
     */
    public ConsistencyCheck( StoreAccess stores, boolean checkPropertyOwners )
    {
        this.nodes = stores.getNodeStore();
        this.rels = stores.getRelationshipStore();
        this.props = stores.getPropertyStore();
        this.strings = stores.getStringStore();
        this.arrays = stores.getArrayStore();
        this.relTypes = stores.getRelationshipTypeStore();
        this.propIndexes = stores.getPropertyIndexStore();
        this.propKeys = stores.getPropertyKeyStore();
        this.typeNames = stores.getTypeNameStore();
        this.propertyOwners = checkPropertyOwners ? new HashMap<Long, PropertyOwner>() : null;
    }

    private static abstract class PropertyOwner
    {
        final long id;

        PropertyOwner( long id )
        {
            this.id = id;
        }

        abstract RecordStore<? extends PrimitiveRecord> storeFrom( ConsistencyCheck tool );

        abstract long otherOwnerOf( PropertyRecord prop );

        abstract long ownerOf( PropertyRecord prop );

        abstract InconsistencyType propertyNotRemoved();
    }

    private static final class OwningNode extends PropertyOwner
    {
        OwningNode( long id )
        {
            super( id );
        }

        @Override
        RecordStore<? extends PrimitiveRecord> storeFrom( ConsistencyCheck tool )
        {
            return tool.nodes;
        }

        @Override
        InconsistencyType propertyNotRemoved()
        {
            return PROPERTY_NOT_REMOVED_FOR_DELETED_NODE;
        }

        @Override
        long otherOwnerOf( PropertyRecord prop )
        {
            return prop.getRelId();
        }

        @Override
        long ownerOf( PropertyRecord prop )
        {
            return prop.getNodeId();
        }
    }

    private static final class OwningRelationship extends PropertyOwner
    {
        OwningRelationship( long id )
        {
            super( id );
        }

        @Override
        RecordStore<? extends PrimitiveRecord> storeFrom( ConsistencyCheck tool )
        {
            return tool.rels;
        }

        @Override
        InconsistencyType propertyNotRemoved()
        {
            return PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP;
        }

        @Override
        long otherOwnerOf( PropertyRecord prop )
        {
            return prop.getNodeId();
        }

        @Override
        long ownerOf( PropertyRecord prop )
        {
            return prop.getRelId();
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public void run()
    {
        ProgressIndicator.MultiProgress progress = progressInit();

        applyFiltered( nodes, progress, RecordStore.IN_USE );
        applyFiltered( rels, progress, RecordStore.IN_USE );
        // free up some heap space that isn't needed anymore
        if ( propertyOwners != null ) propertyOwners.clear();
        applyFiltered( props, progress, RecordStore.IN_USE );
        // free up some heap space that isn't needed anymore
        if ( propertyOwners != null ) propertyOwners.clear();
        applyFiltered( strings, progress, RecordStore.IN_USE );
        applyFiltered( arrays, progress, RecordStore.IN_USE );
        applyFiltered( relTypes, progress, RecordStore.IN_USE );
        applyFiltered( propIndexes, progress, RecordStore.IN_USE );
        applyFiltered( propKeys, progress, RecordStore.IN_USE );
        applyFiltered( typeNames, progress, RecordStore.IN_USE );

        if (progress != null) progress.done();
        checkResult();
    }

    ProgressIndicator.MultiProgress progressInit()
    {
        return null;
    }

    /**
     * Check if any inconsistencies was found by the checker. This method should
     * be invoked at the end of the check. If inconsistencies were found an
     * {@link AssertionError} summarizing the number of inconsistencies will be
     * thrown.
     *
     * @throws AssertionError if any inconsistencies were found.
     */
    public void checkResult() throws AssertionError
    {
        if ( brokenNodes != 0 || brokenRels != 0 || brokenProps != 0 || brokenStrings != 0 || brokenArrays != 0 || brokenTypes != 0 || brokenKeys != 0 )
        {
            throw new AssertionError(
                    String.format(
                            "Store level inconsistency found in %d nodes, %d relationships, %d properties, %d strings, %d arrays, %d types, %d keys",
                            brokenNodes, brokenRels, brokenProps, brokenStrings, brokenArrays, brokenTypes, brokenKeys ) );
        }
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
    public void processString( RecordStore<DynamicRecord> store, DynamicRecord string )
    {
        if ( checkDynamic( store, string ) ) brokenStrings++;
    }

    @Override
    public void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        if ( checkDynamic( store, array ) ) brokenArrays++;
    }

    @Override
    public void processRelationshipType( RecordStore<RelationshipTypeRecord> store, RelationshipTypeRecord type )
    {
        if ( checkType( type ) ) brokenTypes++;
    }

    @Override
    public void processPropertyIndex( RecordStore<PropertyIndexRecord> store, PropertyIndexRecord index )
    {
        if ( checkKey( index ) ) brokenKeys++;
    }

    private boolean checkNode( NodeRecord node )
    {
        boolean fail = false;
        if ( !node.inUse() )
        {
            NodeRecord old = nodes.forceGetRaw( node.getId() );
            if ( old.inUse() ) // Check that referenced records are also removed
            {
                if ( !Record.NO_NEXT_RELATIONSHIP.value( old.getNextRel() ) )
                { // NOTE: with reuse in the same tx this check is invalid
                    RelationshipRecord rel = rels.forceGetRecord( old.getNextRel() );
                    if ( rel.inUse() ) fail |= inconsistent( nodes, node, rels, rel, RELATIONSHIP_NOT_REMOVED_FOR_DELETED_NODE );
                }
                checkPropertyReference( node, nodes, new OwningNode( node.getId() ) );
            }
            return fail;
        }
        long relId = node.getNextRel();
        if ( !Record.NO_NEXT_RELATIONSHIP.value( relId ) )
        {
            RelationshipRecord rel = rels.forceGetRecord( relId );
            if ( !rel.inUse() )
                fail |= inconsistent( nodes, node, rels, rel, RELATIONSHIP_NOT_IN_USE );
            else if ( !( rel.getFirstNode() == node.getId() || rel.getSecondNode() == node.getId() ) )
                fail |= inconsistent( nodes, node, rels, rel, RELATIONSHIP_FOR_OTHER_NODE );
        }
        fail |= checkPropertyReference( node, nodes, new OwningNode( node.getId() ) );
        return fail;
    }

    private <R extends PrimitiveRecord> boolean checkPropertyReference( R primitive, RecordStore<R> store, PropertyOwner owner )
    {
        boolean fail = false;
        if ( props != null )
        {
            if ( primitive.inUse() )
            {
                if ( !Record.NO_NEXT_PROPERTY.value( primitive.getNextProp() ) )
                {
                    PropertyRecord prop = props.forceGetRecord( primitive.getNextProp() );
                    fail |= checkPropertyOwner( prop, owner );
                    if ( !prop.inUse() )
                        fail |= inconsistent( store, primitive, props, prop, PROPERTY_NOT_IN_USE );
                    else if ( owner.otherOwnerOf( prop ) != -1
                              || ( owner.ownerOf( prop ) != -1 && owner.ownerOf( prop ) != primitive.getId() ) )
                        fail |= inconsistent( store, primitive, props, prop, PROPERTY_FOR_OTHER );
                }
            }
            else
            {
                R old = store.forceGetRaw( primitive.getId() );
                if ( !Record.NO_NEXT_PROPERTY.value( old.getNextProp() ) )
                { // NOTE: with reuse in the same tx this check is invalid
                    PropertyRecord prop = props.forceGetRecord( old.getNextProp() );
                    if ( prop.inUse() )
                        fail |= inconsistent( store, primitive, props, prop, owner.propertyNotRemoved() );
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
            RelationshipRecord old = rels.forceGetRaw( rel.getId() );
            if ( old.inUse() )
            {
                for (RelationshipField field : relFields)
                {
                    long otherId = field.relOf( old );
                    if (otherId == field.none)
                    {
                        Long nodeId = field.nodeOf( old );
                        if (nodeId != null)
                        {
                            NodeRecord node = nodes.forceGetRecord( nodeId );
                            if (node.inUse() && node.getNextRel() == old.getId())
                                fail |= inconsistent( rels, rel, nodes, node, REMOVED_RELATIONSHIP_STILL_REFERENCED );
                        }
                    }
                    else
                    {
                        RelationshipRecord other = rels.forceGetRecord( otherId );
                        if (other.inUse() &&field.invConsistent( old, other ))
                            fail |= inconsistent( rels,rel, other, REMOVED_RELATIONSHIP_STILL_REFERENCED );
                    }
                }
                fail |= checkPropertyReference( rel, rels, new OwningRelationship( rel.getId() ) );
            }
            return fail;
        }
        if ( rel.getType() < 0 ) fail |= inconsistent( rels, rel, INVALID_TYPE_ID );
        else
        {
            RelationshipTypeRecord type = relTypes.forceGetRecord( rel.getType() );
            if ( !type.inUse() ) fail |= inconsistent( rels, rel, relTypes, type, TYPE_NOT_IN_USE );
        }
        for ( RelationshipField field : relFields )
        {
            long otherId = field.relOf( rel );
            if ( otherId == field.none )
            {
                Long nodeId = field.nodeOf( rel );
                if ( nodeId != null )
                {
                    NodeRecord node = nodes.forceGetRecord( nodeId );
                    if ( !node.inUse() || node.getNextRel() != rel.getId() )
                        fail |= inconsistent( rels, rel, nodes, node, field.noBackReference );
                }
            }
            else
            {
                RelationshipRecord other = rels.forceGetRecord( otherId );
                if ( !other.inUse() )
                    fail |= inconsistent( rels, rel, other, field.notInUse );
                else if ( !field.invConsistent( rel, other ) )
                    fail |= inconsistent( rels, rel, other, field.differentChain );
            }
        }
        for ( NodeField field : nodeFields )
        {
            long nodeId = field.get( rel );
            if ( nodeId < 0 )
                fail |= inconsistent( rels, rel, field.invalidReference);
            else
            {
                NodeRecord node = nodes.forceGetRecord( nodeId );
                if ( !node.inUse() )
                    fail |= inconsistent( rels, rel, nodes, node, field.notInUse );
            }
        }
        fail |= checkPropertyReference( rel, rels, new OwningRelationship( rel.getId() ) );
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
            RecordStore<PrimitiveRecord> oldStore = (RecordStore<PrimitiveRecord>) oldOwner.storeFrom( this ),
                                         newStore = (RecordStore<PrimitiveRecord>) newOwner.storeFrom( this );
            return inconsistent( oldStore, oldStore.getRecord( oldOwner.id ),
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
            PropertyRecord old = props.forceGetRaw( property.getId() );
            if ( old.inUse() )
            {
                if ( !Record.NO_NEXT_PROPERTY.value( old.getNextProp() ) )
                {
                    PropertyRecord next = props.forceGetRecord( old.getNextProp() );
                    if ( next.inUse() && next.getPrevProp() == old.getId() )
                        fail |= inconsistent( props, property, next, REMOVED_PROPERTY_STILL_REFERENCED );
                }
                if ( !Record.NO_PREVIOUS_PROPERTY.value( old.getPrevProp() ) )
                {
                    PropertyRecord prev = props.forceGetRecord( old.getPrevProp() );
                    if ( prev.inUse() && prev.getNextProp() == old.getId() )
                        fail |= inconsistent( props, property, prev, REMOVED_PROPERTY_STILL_REFERENCED );
                }
                else // property was first in chain
                {
                    if ( property.getNodeId() != -1 )
                        fail |= checkPropertyOwnerReference( property, property.getNodeId(), nodes );
                    else if ( property.getRelId() != -1 )
                        fail |= checkPropertyOwnerReference( property, property.getRelId(), rels );
                    else if ( ((DiffRecordStore<PropertyRecord>)props).isModified( property.getId() ) )
                        fail |= inconsistent( props, property, PROPERTY_CHANGED_WITHOUT_OWNER ); // only a warning
                }
                fail |= checkOwnerChain( property );
            }
            return fail;
        }
        long nextId = property.getNextProp();
        if ( !Record.NO_NEXT_PROPERTY.value( nextId ) )
        {
            PropertyRecord next = props.forceGetRecord( nextId );
            if ( !next.inUse() )
                fail |= inconsistent( props, property, next, NEXT_PROPERTY_NOT_IN_USE );
            if ( next.getPrevProp() != property.getId() )
                fail |= inconsistent( props, property, next, PROPERTY_NEXT_WRONG_BACKREFERENCE );
        }
        long prevId = property.getPrevProp();
        if ( !Record.NO_PREVIOUS_PROPERTY.value( prevId ) )
        {
            PropertyRecord prev = props.forceGetRecord( prevId );
            if ( !prev.inUse() )
                fail |= inconsistent( props, property, prev, PREV_PROPERTY_NOT_IN_USE );
            if ( prev.getNextProp() != property.getId() )
                fail |= inconsistent( props, property, prev, PROPERTY_PREV_WRONG_BACKREFERENCE );
        }
        else // property is first in chain
        {
            if ( property.getNodeId() != -1 )
                fail |= checkPropertyOwnerReference( property, property.getNodeId(), nodes );
            else if ( property.getRelId() != -1 )
                fail |= checkPropertyOwnerReference( property, property.getRelId(), rels );
            else if ( props instanceof DiffRecordStore<?>
                      && ( (DiffRecordStore<PropertyRecord>) props ).isModified( property.getId() ) )
                fail |= inconsistent( props, property, PROPERTY_CHANGED_WITHOUT_OWNER ); // only a warning
            // else - this information is only available from logs through DiffRecordStore
        }
        fail |= checkOwnerChain( property );
        for ( PropertyBlock block : property.getPropertyBlocks() )
        {
            if ( block.getKeyIndexId() < 0 ) fail |= inconsistent( props, property, INVALID_PROPERTY_KEY.forBlock( block ) );
            else
            {
                PropertyIndexRecord key = propIndexes.forceGetRecord( block.getKeyIndexId() );
                if ( !key.inUse() ) fail |= inconsistent( props, property, propIndexes, key, UNUSED_PROPERTY_KEY.forBlock(  block ) );
            }
            RecordStore<DynamicRecord> dynStore = null;
            PropertyType type = block.forceGetType();
            if ( type == null )
            {
                fail |= inconsistent( props, property, ILLEGAL_PROPERTY_TYPE.forBlock( block ) );
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
                    fail |= inconsistent( props, property, dynStore, dynrec, DYNAMIC_NOT_IN_USE.forBlock( block ) );
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
            PrimitiveRecord owner = property.inUse() ? store.forceGetRecord( ownerId ) : store.forceGetRaw( ownerId );
            List<PropertyRecord> chain = new ArrayList<PropertyRecord>( 2 );
            PropertyRecord prop = null;
            for ( long propId = owner.getNextProp(), target = property.getId(); propId != target; propId = prop.getNextProp() )
            {
                if ( Record.NO_NEXT_PROPERTY.value( propId ) )
                {
                    fail |= inconsistent( props, property, store, owner, PROPERTY_CHANGED_FOR_WRONG_OWNER.forProperties( chain ) );
                    break; // chain ended, not found
                }
                prop = property.inUse() ? props.forceGetRecord( propId ) : props.forceGetRaw( propId );
                chain.add( prop );
            }
        }
        else if ( props instanceof DiffRecordStore<?> && ( (DiffRecordStore<?>) props ).isModified( property.getId() ) )
            fail |= inconsistent( props, property, PROPERTY_CHANGED_WITHOUT_OWNER ); // only a warning
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
                    fail |= inconsistent( props, property, entityStore, entity, REMOVED_PROPERTY_STILL_REFERENCED );
            }
            return fail;
        }
        if ( !entity.inUse() )
            fail |= inconsistent( props, property, entityStore, entity, OWNER_NOT_IN_USE );
        else if ( entity.getNextProp() != property.getId() )
            fail |= inconsistent( props, property, entityStore, entity, OWNER_DOES_NOT_REFERENCE_BACK );
        if ( entityStore instanceof DiffRecordStore<?> )
        {
            DiffRecordStore<? extends PrimitiveRecord> diffs = (DiffRecordStore<? extends PrimitiveRecord>) entityStore;
            if ( diffs.isModified( entity.getId() ) )
            {
                PrimitiveRecord old = diffs.forceGetRaw( entity.getId() );
                // IF old is in use and references a property record
                if ( old.inUse() && !Record.NO_NEXT_PROPERTY.value( old.getNextProp() ) )
                    // AND that property record is not the same as this property record
                    if ( old.getNextProp() != property.getId() )
                        // THEN that property record must also have been updated!
                        if ( !( (DiffRecordStore<?>) props ).isModified( old.getNextProp() ) )
                            fail |= inconsistent( props, property, entityStore, entity, REPLACED_PROPERTY );
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
                DynamicRecord old = store.forceGetRaw( record.getId() );
                if ( old.inUse() && !Record.NO_NEXT_BLOCK.value( old.getNextBlock() ) )
                {
                    DynamicRecord next = store.forceGetRecord( old.getNextBlock() );
                    if ( next.inUse() ) // the entire chain must be removed
                        fail |= inconsistent( store, record, next, NEXT_DYNAMIC_NOT_REMOVED );
                }
            }
            return fail;
        }
        long nextId = record.getNextBlock();
        if ( !Record.NO_NEXT_BLOCK.value( nextId ) )
        {
            // If next is set, then it must be in use
            DynamicRecord next = store.forceGetRecord( nextId );
            if ( !next.inUse() )
                fail |= inconsistent( store, record, next, NEXT_DYNAMIC_NOT_IN_USE );
            // If next is set, then the size must be max
            if ( record.getLength() < store.getRecordSize() - store.getRecordHeaderSize() )
                fail |= inconsistent( store, record, NON_FULL_DYNAMIC_WITH_NEXT );
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
            fail |= inconsistent( store, record, DYNAMIC_LENGTH_TOO_LARGE );
        }
        if ( store instanceof DiffRecordStore<?> )
        {
            DiffRecordStore<DynamicRecord> diffs = (DiffRecordStore<DynamicRecord>) store;
            if ( diffs.isModified( record.getId() ) && !record.isLight() )
            { // if the record is really modified it will be heavy
                DynamicRecord prev = diffs.forceGetRaw( record.getId() );
                if ( prev.inUse() ) fail |= inconsistent( store, record, prev, OVERWRITE_USED_DYNAMIC );
            }
        }
        return fail;
    }

    private boolean checkType( RelationshipTypeRecord type )
    {
        if ( !type.inUse() ) return false; // no check for unused records
        if ( Record.NO_NEXT_BLOCK.value( type.getNameId() ) ) return false; // accept this
        DynamicRecord record = typeNames.forceGetRecord( type.getNameId() );
        if ( !record.inUse() ) return inconsistent( relTypes, type, typeNames, record, UNUSED_TYPE_NAME );
        return false;
    }

    private boolean checkKey( PropertyIndexRecord key )
    {
        if ( !key.inUse() ) return false; // no check for unused records
        if ( Record.NO_NEXT_BLOCK.value( key.getNameId() ) ) return false; // accept this
        DynamicRecord record = propKeys.forceGetRecord( key.getNameId() );
        if ( !record.inUse() ) return inconsistent( propIndexes, key, propKeys, record, UNUSED_KEY_NAME );
        return false;
    }

    // Inconsistency between two records
    private <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred, InconsistencyType type )
    {
        report( recordStore, record, referredStore, referred, type );
        return !type.isWarning();
    }

    private <R extends AbstractBaseRecord> boolean inconsistent(
            RecordStore<R> store, R record, R referred, InconsistencyType type )
    {
        report( store, record, store, referred, type );
        return !type.isWarning();
    }

    // Internal inconsistency in a single record
    private <R extends AbstractBaseRecord> boolean inconsistent( RecordStore<R> store, R record, InconsistencyType type )
    {
        report( store, record, type );
        return !type.isWarning();
    }

    /**
     * Report an inconsistency between two records.
     *
     * @param recordStore the store containing the record found to be inconsistent.
     * @param record the record found to be inconsistent.
     * @param referredStore the store containing the record the inconsistent record references.
     * @param referred the record the inconsistent record references.
     * @param inconsistency a description of the inconsistency.
     */
    protected abstract <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
            RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred, InconsistencyType inconsistency );

    /**
     * Report an internal inconsistency in a single record.
     *
     * @param recordStore the store the inconsistent record is stored in.
     * @param record the inconsistent record.
     * @param inconsistency a description of the inconsistency.
     */
    protected abstract <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record, InconsistencyType inconsistency );

    private static NodeField[] nodeFields = NodeField.values();
    private static RelationshipField[] relFields = RelationshipField.values();

    private enum NodeField
    {
        FIRST( SOURCE_NODE_INVALID, SOURCE_NODE_NOT_IN_USE )
        {
            @Override
            long get( RelationshipRecord rel )
            {
                return rel.getFirstNode();
            }
        },
        SECOND( TARGET_NODE_INVALID, TARGET_NODE_NOT_IN_USE )
        {
            @Override
            long get( RelationshipRecord rel )
            {
                return rel.getSecondNode();
            }
        };
        private final ReferenceInconsistency invalidReference, notInUse;

        abstract long get( RelationshipRecord rel );

        private NodeField( ReferenceInconsistency invalidReference, ReferenceInconsistency notInUse )
        {
            this.invalidReference = invalidReference;
            this.notInUse = notInUse;
        }
    }

    @SuppressWarnings( "boxing" )
    private enum RelationshipField
    {
        FIRST_NEXT( true, Record.NO_NEXT_RELATIONSHIP, SOURCE_NEXT_NOT_IN_USE, null, SOURCE_NEXT_DIFFERENT_CHAIN )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getFirstNextRel();
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstPrevRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondPrevRel() == rel.getId();
                return false;
            }
        },
        FIRST_PREV( true, Record.NO_PREV_RELATIONSHIP, SOURCE_PREV_NOT_IN_USE, SOURCE_NO_BACKREF,
                SOURCE_PREV_DIFFERENT_CHAIN )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getFirstPrevRel();
            }

            @Override
            Long nodeOf( RelationshipRecord rel )
            {
                return getNode( rel );
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstNextRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondNextRel() == rel.getId();
                return false;
            }
        },
        SECOND_NEXT( false, Record.NO_NEXT_RELATIONSHIP, TARGET_NEXT_NOT_IN_USE, null, TARGET_NEXT_DIFFERENT_CHAIN )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getSecondNextRel();
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstPrevRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondPrevRel() == rel.getId();
                return false;
            }
        },
        SECOND_PREV( false, Record.NO_PREV_RELATIONSHIP, TARGET_PREV_NOT_IN_USE, TARGET_NO_BACKREF,
                TARGET_PREV_DIFFERENT_CHAIN )
        {
            @Override
            long relOf( RelationshipRecord rel )
            {
                return rel.getSecondPrevRel();
            }

            @Override
            Long nodeOf( RelationshipRecord rel )
            {
                return getNode( rel );
            }

            @Override
            boolean invConsistent( RelationshipRecord rel, RelationshipRecord other )
            {
                long node = getNode( rel );
                if ( other.getFirstNode() == node ) return other.getFirstNextRel() == rel.getId();
                if ( other.getSecondNode() == node ) return other.getSecondNextRel() == rel.getId();
                return false;
            }
        };

        private final ReferenceInconsistency notInUse, noBackReference, differentChain;
        private final boolean first;
        final long none;

        private RelationshipField( boolean first, Record none, ReferenceInconsistency notInUse, ReferenceInconsistency noBackReference, ReferenceInconsistency differentChain )
        {
            this.first = first;
            this.none = none.intValue();
            this.notInUse = notInUse;
            this.noBackReference = noBackReference;
            this.differentChain = differentChain;
        }

        abstract boolean invConsistent( RelationshipRecord rel, RelationshipRecord other );

        long getNode( RelationshipRecord rel )
        {
            return first ? rel.getFirstNode() : rel.getSecondNode();
        }

        abstract long relOf( RelationshipRecord rel );

        Long nodeOf( RelationshipRecord rel )
        {
            return null;
        }
    }
}
