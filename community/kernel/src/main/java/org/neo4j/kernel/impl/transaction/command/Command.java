/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.api.CommandVisitor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyRecordChange;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.util.Bits.bitFlag;
import static org.neo4j.kernel.impl.util.Bits.bitFlags;
import static org.neo4j.kernel.impl.util.IdPrettyPrinter.label;
import static org.neo4j.kernel.impl.util.IdPrettyPrinter.relationshipType;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
public abstract class Command implements StorageCommand
{
    private int keyHash;
    private long key;
    private Mode mode;

    /*
     * TODO: This is techdebt
     * This is used to control the order of how commands are applied, which is done because
     * we don't take read locks, and so the order or how we change things lowers the risk
     * of reading invalid state. This should be removed once eg. MVCC or read locks has been
     * implemented.
     */
    public enum Mode
    {
        CREATE,
        UPDATE,
        DELETE;

        public static Mode fromRecordState( boolean created, boolean inUse )
        {
            if ( !inUse )
            {
                return DELETE;
            }
            if ( created )
            {
                return CREATE;
            }
            return UPDATE;
        }

        public static Mode fromRecordState( AbstractBaseRecord record )
        {
            return fromRecordState( record.isCreated(), record.inUse() );
        }
    }

    /**
     * Many commands have before/after versions of their records. In some scenarios there's a need
     * to parameterize which of those to work with.
     */
    public enum Version
    {
        /**
         * The "before" version of a command's record. I.e. the record how it looked before changes took place.
         */
        BEFORE
        {
            @Override
            <RECORD extends AbstractBaseRecord> RECORD select( BaseCommand<RECORD> command )
            {
                return command.getBefore();
            }
        },
        /**
         * The "after" version of a command's record. I.e. the record how it looks after changes took place.
         */
        AFTER
        {
            @Override
            <RECORD extends AbstractBaseRecord> RECORD select( BaseCommand<RECORD> command )
            {
                return command.getAfter();
            }
        };

        /**
         * Selects one of the versions of a {@link BaseCommand}.
         *
         * @param command command to select a version from.
         * @return the specific record version in this command.
         */
        abstract <RECORD extends AbstractBaseRecord> RECORD select( BaseCommand<RECORD> command );
    }

    protected final void setup( long key, Mode mode )
    {
        this.mode = mode;
        this.keyHash = (int) ((key >>> 32) ^ key);
        this.key = key;
    }

    @Override
    public int hashCode()
    {
        return keyHash;
    }

    // Force implementors to implement toString
    @Override
    public abstract String toString();

    public long getKey()
    {
        return key;
    }

    public Mode getMode()
    {
        return mode;
    }

    @Override
    public boolean equals( Object o )
    {
        return o != null && o.getClass().equals( getClass() ) && getKey() == ((Command) o).getKey();
    }

    public abstract boolean handle( CommandVisitor handler ) throws IOException;

    protected String beforeAndAfterToString( AbstractBaseRecord before, AbstractBaseRecord after )
    {
        return format( " -%s%n         +%s", before, after );
    }

    void writeDynamicRecords( WritableChannel channel, Collection<DynamicRecord> records ) throws IOException
    {
        writeDynamicRecords( channel, records, records.size() );
    }

    void writeDynamicRecords( WritableChannel channel, Iterable<DynamicRecord> records, int size ) throws IOException
    {
        channel.putInt( size ); // 4
        for ( DynamicRecord record : records )
        {
            writeDynamicRecord( channel, record );
        }
    }

    void writeDynamicRecord( WritableChannel channel, DynamicRecord record ) throws IOException
    {
        // id+type+in_use(byte)+nr_of_bytes(int)+next_block(long)
        if ( record.inUse() )
        {
            byte inUse = Record.IN_USE.byteValue();
            if ( record.isStartRecord() )
            {
                inUse |= Record.FIRST_IN_CHAIN.byteValue();
            }
            channel.putLong( record.getId() )
                   .putInt( record.getTypeAsInt() )
                   .put( inUse )
                   .putInt( record.getLength() )
                   .putLong( record.getNextBlock() );
            byte[] data = record.getData();
            assert data != null;
            channel.put( data, data.length );
        }
        else
        {
            byte inUse = Record.NOT_IN_USE.byteValue();
            channel.putLong( record.getId() )
                   .putInt( record.getTypeAsInt() )
                   .put( inUse );
        }
    }

    public abstract static class BaseCommand<RECORD extends AbstractBaseRecord> extends Command
    {
        protected final RECORD before;
        protected final RECORD after;

        public BaseCommand( RECORD before, RECORD after )
        {
            setup( after.getId(), Mode.fromRecordState( after ) );
            this.before = before;
            this.after = after;
        }

        @Override
        public String toString()
        {
            return beforeAndAfterToString( before, after );
        }

        public RECORD getBefore()
        {
            return before;
        }

        public RECORD getAfter()
        {
            return after;
        }
    }

    public static class NodeCommand extends BaseCommand<NodeRecord>
    {
        public NodeCommand( NodeRecord before, NodeRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitNodeCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.NODE_COMMAND );
            channel.putLong( after.getId() );
            writeNodeRecord( channel, before );
            writeNodeRecord( channel, after );
        }

        private void writeNodeRecord( WritableChannel channel, NodeRecord record ) throws IOException
        {
            byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                                   bitFlag( record.isCreated(), Record.CREATED_IN_TX ),
                                   bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                                   bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                                   bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );
            channel.put( flags );
            if ( record.inUse() )
            {
                channel.put( record.isDense() ? (byte) 1 : (byte) 0 );
                channel.putLong( record.getNextRel() ).putLong( record.getNextProp() );
                channel.putLong( record.getLabelField() );
                if ( record.hasSecondaryUnitId() )
                {
                    channel.putLong( record.getSecondaryUnitId() );
                }
            }
            // Always write dynamic label records because we want to know which ones have been deleted
            // especially if the node has been deleted.
            writeDynamicRecords( channel, record.getDynamicLabelRecords() );
        }
    }

    public static class RelationshipCommand extends BaseCommand<RelationshipRecord>
    {
        public RelationshipCommand( RelationshipRecord before, RelationshipRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.REL_COMMAND );
            channel.putLong( after.getId() );
            writeRelationshipRecord( channel, before );
            writeRelationshipRecord( channel, after );
        }

        private void writeRelationshipRecord( WritableChannel channel, RelationshipRecord record ) throws IOException
        {
            byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                                   bitFlag( record.isCreated(), Record.CREATED_IN_TX ),
                                   bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                                   bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                                   bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );
            channel.put( flags );
            if ( record.inUse() )
            {
                channel.putLong( record.getFirstNode() ).putLong( record.getSecondNode() ).putInt( record.getType() )
                       .putLong( record.getFirstPrevRel() ).putLong( record.getFirstNextRel() )
                       .putLong( record.getSecondPrevRel() ).putLong( record.getSecondNextRel() )
                       .putLong( record.getNextProp() )
                       .put( (byte) ((record.isFirstInFirstChain() ? 1 : 0) | (record.isFirstInSecondChain() ? 2 : 0)) );
                if ( record.hasSecondaryUnitId() )
                {
                    channel.putLong( record.getSecondaryUnitId() );
                }
            }
            else
            {
                channel.putInt( record.getType() );
            }
        }
    }

    public static class RelationshipGroupCommand extends BaseCommand<RelationshipGroupRecord>
    {
        public RelationshipGroupCommand( RelationshipGroupRecord before, RelationshipGroupRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipGroupCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.REL_GROUP_COMMAND );
            channel.putLong( after.getId() );
            writeRelationshipGroupRecord( channel, before );
            writeRelationshipGroupRecord( channel, after );
        }

        private void writeRelationshipGroupRecord( WritableChannel channel, RelationshipGroupRecord record )
                throws IOException
        {
            byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                                   bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                                   bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                                   bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );
            channel.put( flags );
            channel.putShort( (short) record.getType() );
            channel.putLong( record.getNext() );
            channel.putLong( record.getFirstOut() );
            channel.putLong( record.getFirstIn() );
            channel.putLong( record.getFirstLoop() );
            channel.putLong( record.getOwningNode() );
            if ( record.hasSecondaryUnitId() )
            {
                channel.putLong( record.getSecondaryUnitId() );
            }
        }
    }

    public static class NeoStoreCommand extends BaseCommand<NeoStoreRecord>
    {
        public NeoStoreCommand( NeoStoreRecord before, NeoStoreRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitNeoStoreCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.NEOSTORE_COMMAND );
            writeNeoStoreRecord( channel, before );
            writeNeoStoreRecord( channel, after );
        }

        private void writeNeoStoreRecord( WritableChannel channel, NeoStoreRecord record ) throws IOException
        {
            channel.putLong( record.getNextProp() );
        }
    }

    public static class PropertyCommand extends BaseCommand<PropertyRecord> implements PropertyRecordChange
    {
        public PropertyCommand( PropertyRecord before, PropertyRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitPropertyCommand( this );
        }

        public long getNodeId()
        {
            return after.getNodeId();
        }

        public long getRelId()
        {
            return after.getRelId();
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.PROP_COMMAND );
            channel.putLong( after.getId() );
            writePropertyRecord( channel, before );
            writePropertyRecord( channel, after );
        }

        private void writePropertyRecord( WritableChannel channel, PropertyRecord record ) throws IOException
        {
            byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                                   bitFlag( record.getRelId() != -1, Record.REL_PROPERTY.byteValue() ),
                                   bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                                   bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                                   bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ) );

            channel.put( flags ); // 1
            channel.putLong( record.getNextProp() ).putLong( record.getPrevProp() ); // 8 + 8
            long nodeId = record.getNodeId();
            long relId = record.getRelId();
            if ( nodeId != -1 )
            {
                channel.putLong( nodeId ); // 8 or
            }
            else if ( relId != -1 )
            {
                channel.putLong( relId ); // 8 or
            }
            else
            {
                // means this records value has not changed, only place in
                // prop chain
                channel.putLong( -1 ); // 8
            }
            if ( record.hasSecondaryUnitId() )
            {
                channel.putLong( record.getSecondaryUnitId() );
            }
            channel.put( (byte) record.numberOfProperties() ); // 1
            for ( PropertyBlock block : record )
            {
                assert block.getSize() > 0 : record + " seems kinda broken";
                writePropertyBlock( channel, block );
            }
            writeDynamicRecords( channel, record.getDeletedRecords() );
        }

        private void writePropertyBlock( WritableChannel channel, PropertyBlock block ) throws IOException
        {
            byte blockSize = (byte) block.getSize();
            assert blockSize > 0 : blockSize + " is not a valid block size value";
            channel.put( blockSize ); // 1
            long[] propBlockValues = block.getValueBlocks();
            for ( long propBlockValue : propBlockValues )
            {
                channel.putLong( propBlockValue );
            }
            /*
             * For each block we need to keep its dynamic record chain if
             * it is just created. Deleted dynamic records are in the property
             * record and dynamic records are never modified. Also, they are
             * assigned as a whole, so just checking the first should be enough.
             */
            if ( block.isLight() )
            {
                /*
                 *  This has to be int. If this record is not light
                 *  then we have the number of DynamicRecords that follow,
                 *  which is an int. We do not currently want/have a flag bit so
                 *  we simplify by putting an int here always
                 */
                channel.putInt( 0 ); // 4 or
            }
            else
            {
                writeDynamicRecords( channel, block.getValueRecords() );
            }
        }
    }

    public abstract static class TokenCommand<RECORD extends TokenRecord> extends BaseCommand<RECORD>
    {
        public TokenCommand( RECORD before, RECORD after )
        {
            super( before, after );
        }

        @Override
        public String toString()
        {
            return beforeAndAfterToString( before, after );
        }
    }

    public static class PropertyKeyTokenCommand extends TokenCommand<PropertyKeyTokenRecord>
    {
        public PropertyKeyTokenCommand( PropertyKeyTokenRecord before, PropertyKeyTokenRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitPropertyKeyTokenCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.PROP_INDEX_COMMAND );
            channel.putInt( after.getIntId() );
            writePropertyKeyTokenRecord( channel, before );
            writePropertyKeyTokenRecord( channel, after );
        }

        private void writePropertyKeyTokenRecord( WritableChannel channel, PropertyKeyTokenRecord record )
                throws IOException
        {
            // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
            channel.put( inUse );
            channel.putInt( record.getPropertyCount() ).putInt( record.getNameId() );
            if ( record.isLight() )
            {
                channel.putInt( 0 );
            }
            else
            {
                writeDynamicRecords( channel, record.getNameRecords() );
            }
        }
    }

    public static class RelationshipTypeTokenCommand extends TokenCommand<RelationshipTypeTokenRecord>
    {
        public RelationshipTypeTokenCommand( RelationshipTypeTokenRecord before,
                RelationshipTypeTokenRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipTypeTokenCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.REL_TYPE_COMMAND );
            channel.putInt( after.getIntId() );
            writeRelationshipTypeTokenRecord( channel, before );
            writeRelationshipTypeTokenRecord( channel, after );
        }

        private void writeRelationshipTypeTokenRecord( WritableChannel channel, RelationshipTypeTokenRecord record )
                throws IOException
        {
            // id+in_use(byte)+count(int)+key_blockId(int)+nr_key_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
            channel.put( inUse );
            channel.putInt( record.getNameId() );
            if ( record.isLight() )
            {
                channel.putInt( 0 );
            }
            else
            {
                writeDynamicRecords( channel, record.getNameRecords() );
            }
        }
    }

    public static class LabelTokenCommand extends TokenCommand<LabelTokenRecord>
    {
        public LabelTokenCommand( LabelTokenRecord before, LabelTokenRecord after )
        {
            super( before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitLabelTokenCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.LABEL_KEY_COMMAND );
            channel.putInt( after.getIntId() );
            writeLabelTokenRecord( channel, before );
            writeLabelTokenRecord( channel, after );
        }

        private void writeLabelTokenRecord( WritableChannel channel, LabelTokenRecord record ) throws IOException
        {
            // id+in_use(byte)+type_blockId(int)+nr_type_records(int)
            byte inUse = record.inUse() ? Record.IN_USE.byteValue() : Record.NOT_IN_USE.byteValue();
            channel.put( inUse ).putInt( record.getNameId() );
            writeDynamicRecords( channel, record.getNameRecords() );
        }
    }

    public static class SchemaRuleCommand extends Command
    {
        private final SchemaRecord recordsBefore;
        private final SchemaRecord recordsAfter;
        private final SchemaRule schemaRule;

        public SchemaRuleCommand( Collection<DynamicRecord> recordsBefore, Collection<DynamicRecord> recordsAfter,
                SchemaRule schemaRule )
        {
            this( new SchemaRecord( recordsBefore ), new SchemaRecord( recordsAfter ), schemaRule );
        }

        public SchemaRuleCommand( SchemaRecord recordsBefore, SchemaRecord recordsAfter,
                SchemaRule schemaRule )
        {
            setup( Iterables.first( recordsAfter ).getId(), Mode.fromRecordState( Iterables.first( recordsAfter ) ) );
            this.recordsBefore = recordsBefore;
            this.recordsAfter = recordsAfter;
            this.schemaRule = schemaRule;
        }

        @Override
        public String toString()
        {
            if ( schemaRule != null )
            {
                return getMode() + ":" + schemaRule.toString();
            }
            return "SchemaRule" + recordsAfter;
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitSchemaRuleCommand( this );
        }

        public SchemaRecord getRecordsAfter()
        {
            return recordsAfter;
        }

        public SchemaRule getSchemaRule()
        {
            return schemaRule;
        }

        public SchemaRecord getRecordsBefore()
        {
            return recordsBefore;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.SCHEMA_RULE_COMMAND );
            writeDynamicRecords( channel, recordsBefore, recordsBefore.size() );
            writeDynamicRecords( channel, recordsAfter, recordsAfter.size() );
            channel.put( Iterables.first( recordsAfter ).isCreated() ? (byte) 1 : 0 );
        }
    }

    public static class NodeCountsCommand extends Command
    {
        private final int labelId;
        private final long delta;

        public NodeCountsCommand( int labelId, long delta )
        {
            setup( labelId, Mode.UPDATE );
            assert delta != 0 : "Tried to create a NodeCountsCommand for something that didn't change any count";
            this.labelId = labelId;
            this.delta = delta;
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s) %s %d]",
                    label( labelId ), delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitNodeCountsCommand( this );
        }

        public int labelId()
        {
            return labelId;
        }

        public long delta()
        {
            return delta;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.UPDATE_NODE_COUNTS_COMMAND );
            channel.putInt( labelId() )
                   .putLong( delta() );
        }
    }

    public static class RelationshipCountsCommand extends Command
    {
        private final int startLabelId;
        private final int typeId;
        private final int endLabelId;
        private final long delta;

        public RelationshipCountsCommand( int startLabelId, int typeId, int endLabelId, long delta )
        {
            setup( typeId, Mode.UPDATE );
            assert delta !=
                   0 : "Tried to create a RelationshipCountsCommand for something that didn't change any count";
            this.startLabelId = startLabelId;
            this.typeId = typeId;
            this.endLabelId = endLabelId;
            this.delta = delta;
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s)-%s->(%s) %s %d]",
                    label( startLabelId ), relationshipType( typeId ), label( endLabelId ),
                    delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipCountsCommand( this );
        }

        public int startLabelId()
        {
            return startLabelId;
        }

        public int typeId()
        {
            return typeId;
        }

        public int endLabelId()
        {
            return endLabelId;
        }

        public long delta()
        {
            return delta;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.put( NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND );
            channel.putInt( startLabelId() )
                   .putInt( typeId() )
                   .putInt( endLabelId() )
                   .putLong( delta() );
        }
    }
}
