/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.backup;

import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;

class ConsistencyCheck extends RecordStore.Processor implements Runnable
{
    private final RecordStore<NodeRecord> nodes;
    private final RecordStore<RelationshipRecord> rels;
    private final RecordStore<PropertyRecord> props;
    private final RecordStore<DynamicRecord> strings;
    private final RecordStore<DynamicRecord> arrays;
    /* TODO:
    private final PropertyIndexStore  propIndexStore;
    private final RelationshipTypeStore  relTypeStore;
    //*/
    private long brokenNodes, brokenRels, brokenProps, brokenStrings, brokenArrays;

    ConsistencyCheck( RecordStore<NodeRecord> nodes, RecordStore<RelationshipRecord> rels,
            RecordStore<PropertyRecord> props, RecordStore<DynamicRecord> strings, RecordStore<DynamicRecord> arrays )
    {
        this.nodes = nodes;
        this.rels = rels;
        this.props = props;
        this.strings = strings;
        this.arrays = arrays;
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public void run()
    {
        apply( nodes, RecordStore.IN_USE );
        apply( rels, RecordStore.IN_USE );
        apply( props, RecordStore.IN_USE );
        apply( strings, RecordStore.IN_USE );
        apply( arrays, RecordStore.IN_USE );
        if ( brokenNodes != 0 || brokenRels != 0 || brokenProps != 0 || brokenStrings != 0 || brokenArrays != 0 )
        {
            throw new AssertionError(
                    String.format(
                            "Store level inconsistency found in %d nodes, %d relationships, %d properties, %d strings, %d arrays",
                            brokenNodes, brokenRels, brokenProps, brokenStrings, brokenArrays ) );
        }
    }

    @Override
    protected void processNode( RecordStore<NodeRecord> store, NodeRecord node )
    {
        if ( checkNode( node ) ) brokenNodes++;
    }

    @Override
    protected void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
    {
        if ( checkRelationship( rel ) ) brokenRels++;
    }

    @Override
    protected void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
    {
        if ( checkProperty( property ) ) brokenProps++;
    }

    @Override
    protected void processString( RecordStore<DynamicRecord> store, DynamicRecord string )
    {
        if ( checkDynamic( store, string ) ) brokenStrings++;
    }

    @Override
    protected void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        if ( checkDynamic( store, array ) ) brokenArrays++;
    }

    private boolean checkNode( NodeRecord node )
    {
        boolean fail = false;
        long relId = node.getNextRel();
        if ( Record.NO_NEXT_RELATIONSHIP.value( relId ) )
        {
            RelationshipRecord rel = rels.forceGetRecord( relId );
            if ( !rel.inUse() || !( rel.getFirstNode() == node.getId() || rel.getSecondNode() == node.getId() ) )
            {
                fail = report( node, rel, "invalid relationship reference" );
            }
        }
        if ( props != null )
        {
            long propId = node.getNextProp();
            if ( Record.NO_NEXT_PROPERTY.value( propId ) )
            {
                PropertyRecord prop = props.forceGetRecord( propId );
                if ( !prop.inUse() ) fail = report( node, prop, "invalid property reference" );
            }
        }
        return fail;
    }

    private boolean checkRelationship( RelationshipRecord rel )
    {
        boolean fail = false;
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
                    {
                        fail = report( rel, node, "invalid " + field.name() + " reference" );
                    }
                }
            }
            else
            {
                RelationshipRecord other = rels.forceGetRecord( otherId );
                if ( !other.inUse() || !field.invConsistent( rel, other ) )
                {
                    fail = report( rel, other, "invalid " + field.name() + " reference" );
                }
            }
        }
        for ( NodeField field : nodeFields )
        {
            long nodeId = field.get( rel );
            if ( nodeId < 0 )
                fail = report( rel, "invalid " + field.name() + " node reference" );
            else
            {
                NodeRecord node = nodes.forceGetRecord( nodeId );
                if ( !node.inUse() )
                {
                    fail = report( rel, node, "invalid " + field.name() + " node reference" );
                }
            }
        }
        if ( props != null )
        {
            long propId = rel.getNextProp();
            if ( Record.NO_NEXT_PROPERTY.value( propId ) )
            {
                PropertyRecord prop = props.forceGetRecord( propId );
                if ( !prop.inUse() || !Record.NO_PREVIOUS_PROPERTY.value( prop.getPrevProp() ) )
                    fail = report( rel, prop, "invalid property reference" );
            }
        }
        return fail;
    }

    private boolean checkProperty( PropertyRecord property )
    {
        boolean fail = false;
        long nextId = property.getNextProp();
        if ( Record.NO_NEXT_PROPERTY.value( nextId ) )
        {
            PropertyRecord next = props.forceGetRecord( nextId );
            if ( !next.inUse() || next.getPrevProp() != property.getId() )
            {
                fail = report( property, next, "invalid next reference" );
            }
        }
        long prevId = property.getPrevProp();
        if ( Record.NO_PREVIOUS_PROPERTY.value( prevId ) )
        {
            PropertyRecord prev = props.forceGetRecord( prevId );
            if ( !prev.inUse() || prev.getNextProp() != property.getId() )
            {
                fail = report( property, prev, "invalid previous reference" );
            }
        }
        for ( PropertyBlock block : property.getPropertyBlocks() )
        {
            RecordStore<DynamicRecord> dynStore = null;
            PropertyType type = block.forceGetType();
            if ( type == null )
            {
                fail = report( property, "illegal property type" );
            }
            else
                switch ( block.getType() )
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
                {
                    fail = report( property, dynrec, "first dynamic record not in use" );
                }
            }
        }
        return fail;
    }

    private boolean checkDynamic( RecordStore<DynamicRecord> store, DynamicRecord record )
    {
        boolean fail = false;
        long nextId = record.getNextBlock();
        if ( Record.NO_NEXT_BLOCK.value( nextId ) )
        {
            DynamicRecord next = store.forceGetRecord( nextId );
            if ( !next.inUse() )
            {
                fail = report( record, next, "next record not in use" );
            }
        }
        return fail;
    }

    // Inconsistency between two records
    boolean report( Abstract64BitRecord record, Abstract64BitRecord referred, String message )
    {
        System.err.println( record + " " + referred + " //" + message );
        return true;
    }

    // Internal inconsistency in a single record
    boolean report( Abstract64BitRecord record, String message )
    {
        System.err.println( record + " //" + message );
        return true;
    }

    private static NodeField[] nodeFields = NodeField.values();
    private static RelationshipField[] relFields = RelationshipField.values();

    private enum NodeField
    {
        FIRST
        {
            @Override
            long get( RelationshipRecord rel )
            {
                return rel.getFirstNode();
            }
        },
        SECOND
        {
            @Override
            long get( RelationshipRecord rel )
            {
                return rel.getSecondNode();
            }
        };

        abstract long get( RelationshipRecord rel );
    }

    @SuppressWarnings( "boxing" )
    private enum RelationshipField
    {
        FIRST_NEXT( true, Record.NO_NEXT_RELATIONSHIP )
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
        FIRST_PREV( true, Record.NO_PREV_RELATIONSHIP )
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
        SECOND_NEXT( false, Record.NO_NEXT_RELATIONSHIP )
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
        SECOND_PREV( false, Record.NO_PREV_RELATIONSHIP )
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

        private final boolean first;
        final long none;

        private RelationshipField( boolean first, Record none )
        {
            this.first = first;
            this.none = none.intValue();
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
