/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyRecordChange;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableCollection;

import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.impl.util.IdPrettyPrinter.label;
import static org.neo4j.kernel.impl.util.IdPrettyPrinter.relationshipType;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
public abstract class Command
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

    protected final void setup( long key, Mode mode )
    {
        this.mode = mode;
        this.keyHash = (int) (( key >>> 32 ) ^ key );
        this.key = key;
    }

    public abstract void accept( CommandRecordVisitor visitor );

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

    public abstract boolean handle( NeoCommandHandler handler ) throws IOException;

    protected String beforeAndAfterToString( AbstractBaseRecord before, AbstractBaseRecord after )
    {
        return format( " -%s%n         +%s", before, after );
    }

    public static class NodeCommand extends Command
    {
        private NodeRecord before;
        private NodeRecord after;

        public NodeCommand init( NodeRecord before, NodeRecord after )
        {
            setup( after.getId(), Mode.fromRecordState( after ) );
            this.before = before;
            this.after = after;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitNode( after );
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitNodeCommand( this );
        }

        @Override
        public String toString()
        {
            return beforeAndAfterToString( before, after );
        }

        public NodeRecord getBefore()
        {
            return before;
        }

        public NodeRecord getAfter()
        {
            return after;
        }
    }

    public static class RelationshipCommand extends Command
    {
        private RelationshipRecord record;

        public RelationshipCommand init( RelationshipRecord record )
        {
            setup( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationship( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitRelationshipCommand( this );
        }

        public RelationshipRecord getRecord()
        {
            return record;
        }
    }

    public static class RelationshipGroupCommand extends Command
    {
        private RelationshipGroupRecord record;

        public RelationshipGroupCommand init( RelationshipGroupRecord record )
        {
            setup( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationshipGroup( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitRelationshipGroupCommand( this );
        }

        public RelationshipGroupRecord getRecord()
        {
            return record;
        }
    }

    public static class NeoStoreCommand extends Command
    {
        private NeoStoreRecord record;

        public NeoStoreCommand init( NeoStoreRecord record )
        {
            if( record != null )
            {
                setup( record.getId(), Mode.fromRecordState( record ) );
            }
            this.record = record;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitNeoStore( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitNeoStoreCommand( this );
        }

        public NeoStoreRecord getRecord()
        {
            return record;
        }
    }

    public static class PropertyKeyTokenCommand extends TokenCommand<PropertyKeyTokenRecord>
    {
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitPropertyKeyToken( record );
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitPropertyKeyTokenCommand( this );
        }
    }

    public static class PropertyCommand extends Command implements PropertyRecordChange
    {
        private PropertyRecord before;
        private PropertyRecord after;

        // TODO as optimization the deserialized key/values could be passed in here
        // so that the cost of deserializing them only applies in recovery/HA
        public PropertyCommand init( PropertyRecord before, PropertyRecord after )
        {
            setup( after.getId(), Mode.fromRecordState( after ) );
            this.before = before;
            this.after = after;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitProperty( after );
        }

        @Override
        public String toString()
        {
            return beforeAndAfterToString( before, after );
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitPropertyCommand( this );
        }

        @Override
        public PropertyRecord getBefore()
        {
            return before;
        }

        @Override
        public PropertyRecord getAfter()
        {
            return after;
        }

        public long getNodeId()
        {
            return after.getNodeId();
        }

        public long getRelId()
        {
            return after.getRelId();
        }
    }

    public static abstract class TokenCommand<RECORD extends TokenRecord> extends Command
    {
        protected RECORD record;

        public TokenCommand<RECORD> init( RECORD record )
        {
            setup( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            return this;
        }

        public RECORD getRecord()
        {
            return record;
        }

        @Override
        public String toString()
        {
            return record.toString();
        }
    }

    public static class RelationshipTypeTokenCommand extends TokenCommand<RelationshipTypeTokenRecord>
    {
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationshipTypeToken( record );
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitRelationshipTypeTokenCommand( this );
        }
    }

    public static class LabelTokenCommand extends TokenCommand<LabelTokenRecord>
    {
        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitLabelToken( record );
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitLabelTokenCommand( this );
        }
    }

    public static class SchemaRuleCommand extends Command
    {
        private Collection<DynamicRecord> recordsBefore;
        private Collection<DynamicRecord> recordsAfter;
        private SchemaRule schemaRule;

        public SchemaRuleCommand init( Collection<DynamicRecord> recordsBefore,
                           Collection<DynamicRecord> recordsAfter, SchemaRule schemaRule )
        {
            setup( first( recordsAfter ).getId(), Mode.fromRecordState( first( recordsAfter ) ) );
            this.recordsBefore = recordsBefore;
            this.recordsAfter = recordsAfter;
            this.schemaRule = schemaRule;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitSchemaRule( recordsAfter );
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
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitSchemaRuleCommand( this );
        }

        public Collection<DynamicRecord> getRecordsAfter()
        {
            return unmodifiableCollection( recordsAfter );
        }

        public SchemaRule getSchemaRule()
        {
            return schemaRule;
        }

        public Collection<DynamicRecord> getRecordsBefore()
        {
            return recordsBefore;
        }
    }

    public static class NodeCountsCommand extends Command
    {
        private int labelId;
        private long delta;

        public NodeCountsCommand init( int labelId, long delta )
        {
            setup( labelId, Mode.UPDATE );
            assert delta != 0 : "Tried to create a NodeCountsCommand for something that didn't change any count";
            this.labelId = labelId;
            this.delta = delta;
            return this;
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s) %s %d]",
                                  label( labelId ), delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitNodeCountsCommand( this );
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            // no record to visit
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
        private int startLabelId;
        private int typeId;
        private int endLabelId;
        private long delta;

        public RelationshipCountsCommand init( int startLabelId, int typeId, int endLabelId, long delta )
        {
            setup( typeId, Mode.UPDATE );
            assert delta != 0 : "Tried to create a RelationshipCountsCommand for something that didn't change any count";
            this.startLabelId = startLabelId;
            this.typeId = typeId;
            this.endLabelId = endLabelId;
            this.delta = delta;
            return this;
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s)-%s->(%s) %s %d]",
                    label( startLabelId ), relationshipType( typeId ), label( endLabelId ),
                    delta < 0 ? "-" : "+", Math.abs( delta ) );
        }

        @Override
        public boolean handle( NeoCommandHandler handler ) throws IOException
        {
            return handler.visitRelationshipCountsCommand( this );
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            // no record to visit
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
}
