/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.recordstorage;

import java.io.IOException;

import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockType;
import org.neo4j.storageengine.api.Mask;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory.LATEST_LOG_SERIALIZATION;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.token.api.TokenIdPrettyPrinter.label;
import static org.neo4j.token.api.TokenIdPrettyPrinter.relationshipType;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
public abstract class Command implements StorageCommand
{
    private static final int RECOVERY_LOCK_TYPE_PROPERTY = 0;
    private static final int RECOVERY_LOCK_TYPE_PROPERTY_DYNAMIC = 1;
    private static final int RECOVERY_LOCK_TYPE_NODE_LABEL_DYNAMIC = 2;
    private static final int RECOVERY_LOCK_TYPE_RELATIONSHIP_GROUP = 3;

    protected final LogCommandSerialization serialization;
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

    public Command()
    {
        this( LATEST_LOG_SERIALIZATION );
    }

    public Command( LogCommandSerialization serialization )
    {
        this.serialization = serialization;
    }

    protected final void setup( long key, Mode mode )
    {
        this.mode = mode;
        this.keyHash = (int) ((key >>> 32) ^ key);
        this.key = key;
    }

    @Override
    public KernelVersion version()
    {
        return serialization.version();
    }

    @Override
    public int hashCode()
    {
        return keyHash;
    }

    @Override
    public final String toString()
    {
        return toString( Mask.NO );
    }

    // Force implementors to implement toString( mask )
    @Override
    public abstract String toString( Mask mask );

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

    void lockForRecovery( LockService lockService, LockGroup lockGroup, TransactionApplicationMode mode )
    {
        // most commands does not need this locking
    }

    protected static String beforeAndAfterToString( AbstractBaseRecord before, AbstractBaseRecord after, Mask mask )
    {
        return format( "\t-%s%n\t+%s", before.toString( mask ), after.toString( mask ) );
    }

    public abstract static class BaseCommand<RECORD extends AbstractBaseRecord> extends Command
    {
        protected final RECORD before;
        protected final RECORD after;

        public BaseCommand( RECORD before, RECORD after )
        {
            this( LATEST_LOG_SERIALIZATION, before, after );
        }

        public BaseCommand( LogCommandSerialization serialization, RECORD before, RECORD after )
        {
            super( serialization );
            setup( after.getId(), Mode.fromRecordState( after ) );
            this.before = before;
            this.after = after;
        }

        @Override
        public String toString( Mask mask )
        {
            return beforeAndAfterToString( before, after, mask );
        }

        public RECORD getBefore()
        {
            return before;
        }

        public RECORD getAfter()
        {
            return after;
        }

        RECORD record( TransactionApplicationMode mode )
        {
            return mode == TransactionApplicationMode.REVERSE_RECOVERY ? before : after;
        }
    }

    public static class NodeCommand extends BaseCommand<NodeRecord>
    {
        static final long HEAP_SIZE = shallowSizeOfInstance( NodeCommand.class );

        public NodeCommand( NodeRecord before, NodeRecord after )
        {
            super( before, after );
        }

        public NodeCommand( LogCommandSerialization serialization, NodeRecord before, NodeRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitNodeCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeNodeCommand( channel, this );
        }

        @Override
        void lockForRecovery( LockService lockService, LockGroup locks, TransactionApplicationMode mode )
        {
            locks.add( lockService.acquireNodeLock( getKey(), LockType.EXCLUSIVE ) );
            for ( DynamicRecord dynamicLabelRecord : record( mode ).getDynamicLabelRecords() )
            {
                locks.add( lockService.acquireCustomLock( RECOVERY_LOCK_TYPE_NODE_LABEL_DYNAMIC, dynamicLabelRecord.getId(), LockType.EXCLUSIVE ) );
            }
        }
    }

    public static class RelationshipCommand extends BaseCommand<RelationshipRecord>
    {
        static final long HEAP_SIZE = shallowSizeOfInstance( RelationshipCommand.class );

        public RelationshipCommand( RelationshipRecord before, RelationshipRecord after )
        {
            super( before, after );
        }

        public RelationshipCommand( LogCommandSerialization serialization, RelationshipRecord before, RelationshipRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeRelationshipCommand( channel, this );
        }

        @Override
        void lockForRecovery( LockService lockService, LockGroup locks, TransactionApplicationMode mode )
        {
            locks.add( lockService.acquireRelationshipLock( getKey(), LockType.EXCLUSIVE ) );
        }
    }

    public static class RelationshipGroupCommand extends BaseCommand<RelationshipGroupRecord>
    {
        static final long HEAP_SIZE = shallowSizeOfInstance( RelationshipGroupCommand.class );

        public RelationshipGroupCommand( RelationshipGroupRecord before, RelationshipGroupRecord after )
        {
            super( before, after );
        }

        public RelationshipGroupCommand( LogCommandSerialization serialization, RelationshipGroupRecord before, RelationshipGroupRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipGroupCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeRelationshipGroupCommand( channel, this );
        }

        @Override
        void lockForRecovery( LockService lockService, LockGroup locks, TransactionApplicationMode mode )
        {
            locks.add( lockService.acquireNodeLock( after.getOwningNode(), LockType.EXCLUSIVE ) );
            if ( getMode() == Mode.CREATE || getMode() == Mode.DELETE )
            {
                // This lock on the property guards for reuse of this property
                locks.add( lockService.acquireCustomLock( RECOVERY_LOCK_TYPE_RELATIONSHIP_GROUP, after.getId(), LockType.EXCLUSIVE ) );
            }
        }
    }

    // Command that was used for graph properties.
    // Here only for compatibility reasons for older versions (before 4.0)
    @Deprecated( forRemoval = true )
    public static class NeoStoreCommand extends BaseCommand<NeoStoreRecord>
    {
        NeoStoreCommand( NeoStoreRecord before, NeoStoreRecord after )
        {
            super( before, after );
        }

        NeoStoreCommand( LogCommandSerialization serialization, NeoStoreRecord before, NeoStoreRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeNeoStoreCommand( channel, this );
        }
    }

    public static class MetaDataCommand extends BaseCommand<MetaDataRecord>
    {
        MetaDataCommand( MetaDataRecord before, MetaDataRecord after )
        {
            super( before, after );
        }

        MetaDataCommand( LogCommandSerialization serialization, MetaDataRecord before, MetaDataRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitMetaDataCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeMetaDataCommand( channel, this );
        }
    }

    public static class PropertyCommand extends BaseCommand<PropertyRecord> implements PropertyRecordChange
    {
        static final long HEAP_SIZE = shallowSizeOfInstance( PropertyCommand.class );

        public PropertyCommand( PropertyRecord before, PropertyRecord after )
        {
            super( before, after );
        }

        public PropertyCommand( LogCommandSerialization serialization, PropertyRecord before, PropertyRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitPropertyCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writePropertyCommand( channel, this );
        }

        public long getEntityId()
        {
            return after.isNodeSet() ? after.getNodeId() : after.getRelId();
        }

        public long getNodeId()
        {
            return after.getNodeId();
        }

        public long getRelId()
        {
            return after.getRelId();
        }

        public long getSchemaRuleId()
        {
            return after.getSchemaRuleId();
        }

        @Override
        void lockForRecovery( LockService lockService, LockGroup locks, TransactionApplicationMode mode )
        {
            if ( after.isNodeSet() )
            {
                locks.add( lockService.acquireNodeLock( getNodeId(), LockType.EXCLUSIVE ) );
            }
            else
            {
                locks.add( lockService.acquireRelationshipLock( getRelId(), LockType.EXCLUSIVE ) );
            }

            // Guard for reuse of these records
            PropertyRecord record = record( mode );
            for ( DynamicRecord deletedRecord : record.getDeletedRecords() )
            {
                locks.add( lockService.acquireCustomLock( RECOVERY_LOCK_TYPE_PROPERTY_DYNAMIC, deletedRecord.getId(), LockType.EXCLUSIVE ) );
            }
            for ( PropertyBlock block : record )
            {
                for ( DynamicRecord valueRecord : block.getValueRecords() )
                {
                    locks.add( lockService.acquireCustomLock( RECOVERY_LOCK_TYPE_PROPERTY_DYNAMIC, valueRecord.getId(), LockType.EXCLUSIVE ) );
                }
            }
            if ( getMode() == Mode.CREATE || getMode() == Mode.DELETE )
            {
                locks.add( lockService.acquireCustomLock( RECOVERY_LOCK_TYPE_PROPERTY, after.getId(), LockType.EXCLUSIVE ) );
            }
        }
    }

    public abstract static class TokenCommand<RECORD extends TokenRecord> extends BaseCommand<RECORD> implements StorageCommand.TokenCommand
    {
        public TokenCommand( RECORD before, RECORD after )
        {
            super( before, after );
        }

        public TokenCommand( LogCommandSerialization serialization, RECORD before, RECORD after )
        {
            super( serialization, before, after );
        }

        @Override
        public int tokenId()
        {
            return toIntExact( getKey() );
        }

        @Override
        public boolean isInternal()
        {
            return getAfter().isInternal();
        }

        @Override
        public String toString( Mask mask )
        {
            return beforeAndAfterToString( before, after, mask );
        }
    }

    public static class PropertyKeyTokenCommand extends TokenCommand<PropertyKeyTokenRecord>
    {
        static final long HEAP_SIZE = shallowSizeOfInstance( PropertyKeyTokenCommand.class );

        public PropertyKeyTokenCommand( PropertyKeyTokenRecord before, PropertyKeyTokenRecord after )
        {
            super( before, after );
        }

        public PropertyKeyTokenCommand( LogCommandSerialization serialization, PropertyKeyTokenRecord before, PropertyKeyTokenRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitPropertyKeyTokenCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writePropertyKeyTokenCommand( channel, this );
        }
    }

    public static class RelationshipTypeTokenCommand extends TokenCommand<RelationshipTypeTokenRecord>
    {
        static final long HEAP_SIZE = shallowSizeOfInstance( RelationshipTypeTokenCommand.class );

        public RelationshipTypeTokenCommand( RelationshipTypeTokenRecord before, RelationshipTypeTokenRecord after )
        {
            super( before, after );
        }

        public RelationshipTypeTokenCommand( LogCommandSerialization serialization, RelationshipTypeTokenRecord before, RelationshipTypeTokenRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitRelationshipTypeTokenCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeRelationshipTypeTokenCommand( channel, this );
        }
    }

    public static class LabelTokenCommand extends TokenCommand<LabelTokenRecord>
    {
        static final long HEAP_SIZE = shallowSizeOfInstance( LabelTokenCommand.class );

        public LabelTokenCommand( LabelTokenRecord before, LabelTokenRecord after )
        {
            super( before, after );
        }

        public LabelTokenCommand( LogCommandSerialization serialization, LabelTokenRecord before, LabelTokenRecord after )
        {
            super( serialization, before, after );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitLabelTokenCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeLabelTokenCommand( channel, this );
        }
    }

    public static class SchemaRuleCommand extends BaseCommand<SchemaRecord>
    {
        private final SchemaRule schemaRule;

        static final long HEAP_SIZE = shallowSizeOfInstance( SchemaRuleCommand.class );

        public SchemaRuleCommand( SchemaRecord recordBefore, SchemaRecord recordAfter, SchemaRule schemaRule )
        {
            this( LATEST_LOG_SERIALIZATION, recordBefore, recordAfter, schemaRule );
        }

        public SchemaRuleCommand( LogCommandSerialization serialization, SchemaRecord recordBefore, SchemaRecord recordAfter, SchemaRule schemaRule )
        {
            super( serialization, recordBefore, recordAfter );
            this.schemaRule = schemaRule;
        }

        @Override
        public String toString( Mask mask )
        {
            String beforeAndAfterRecords = super.toString( mask );
            if ( schemaRule != null )
            {
                return beforeAndAfterRecords + " : " + schemaRule;
            }
            return beforeAndAfterRecords;
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitSchemaRuleCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeSchemaRuleCommand( channel, this );
        }

        public SchemaRule getSchemaRule()
        {
            return schemaRule;
        }
    }

    public static class NodeCountsCommand extends Command
    {
        static final long SHALLOW_SIZE = shallowSizeOfInstance( NodeCountsCommand.class );

        private final int labelId;
        private final long delta;

        public NodeCountsCommand( int labelId, long delta )
        {
            this( LATEST_LOG_SERIALIZATION, labelId, delta );
        }

        public NodeCountsCommand( LogCommandSerialization serialization, int labelId, long delta )
        {
            super( serialization );
            setup( labelId, Mode.UPDATE );
            assert delta != 0 : "Tried to create a NodeCountsCommand for something that didn't change any count";
            this.labelId = labelId;
            this.delta = delta;
        }

        @Override
        public String toString( Mask mask )
        {
            return String.format( "UpdateCounts[(%s) %s %d]",
                    label( labelId ), delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitNodeCountsCommand( this );
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeNodeCountsCommand( channel, this );
        }

        public int labelId()
        {
            return labelId;
        }

        public long delta()
        {
            return delta;
        }
    }

    public static class RelationshipCountsCommand extends Command
    {
        static final long SHALLOW_SIZE = shallowSizeOfInstance( RelationshipCountsCommand.class );

        private final int startLabelId;
        private final int typeId;
        private final int endLabelId;
        private final long delta;

        public RelationshipCountsCommand( int startLabelId, int typeId, int endLabelId, long delta )
        {
            this( LATEST_LOG_SERIALIZATION, startLabelId, typeId, endLabelId, delta );
        }

        public RelationshipCountsCommand( LogCommandSerialization serialization, int startLabelId, int typeId, int endLabelId, long delta )
        {
            super( serialization );
            setup( typeId, Mode.UPDATE );
            assert delta !=
                   0 : "Tried to create a RelationshipCountsCommand for something that didn't change any count";
            this.startLabelId = startLabelId;
            this.typeId = typeId;
            this.endLabelId = endLabelId;
            this.delta = delta;
        }

        @Override
        public String toString( Mask mask )
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

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeRelationshipCountsCommand( channel, this );
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
    }

    public static class GroupDegreeCommand extends Command
    {
        static final long SHALLOW_SIZE = shallowSizeOfInstance( GroupDegreeCommand.class );

        private final long groupId;
        private final RelationshipDirection direction;
        private final long delta;

        public GroupDegreeCommand( long groupId, RelationshipDirection direction, long delta )
        {
            setup( combinedKeyOnGroupAndDirection( groupId, direction ), Mode.UPDATE );
            assert delta != 0 : "Tried to create a GroupDegreeCommand for something that didn't change any count";
            this.groupId = groupId;
            this.direction = direction;
            this.delta = delta;
        }

        @Override
        public String toString( Mask mask )
        {
            return String.format( "GroupDegree[(group:%s, %s) %s %d]", groupId, direction, delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( CommandVisitor handler ) throws IOException
        {
            return handler.visitGroupDegreeCommand( this );
        }

        public long groupId()
        {
            return groupId;
        }

        public RelationshipDirection direction()
        {
            return direction;
        }

        public long delta()
        {
            return delta;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            serialization.writeGroupDegreeCommand( channel, this );
        }

        public static long combinedKeyOnGroupAndDirection( long groupId, RelationshipDirection direction )
        {
            return groupId << 2 | direction.id();
        }

        public static long groupIdFromCombinedKey( long key )
        {
            return key >> 2;
        }

        public static RelationshipDirection directionFromCombinedKey( long key )
        {
            return RelationshipDirection.ofId( (int) (key & 0x3) );
        }
    }
}
