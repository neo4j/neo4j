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
package org.neo4j.kernel.impl.nioneo.xa.command;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.xa.PropertyRecordChange;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

import static java.util.Collections.unmodifiableCollection;
import static org.neo4j.helpers.collection.IteratorUtil.first;

/**
 * Command implementations for all the commands that can be performed on a Neo
 * store.
 */
public abstract class Command extends XaCommand
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

    public abstract boolean accept( NeoCommandVisitor visitor ) throws IOException;

    public abstract void applyToCache( CacheAccessBackDoor cacheAccess );

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

        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitNodeCommand( this );
        }

        @Override
        public String toString()
        {
            return after.toString();
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeNodeFromCache( getKey() );
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
        // before update stores the record as it looked before the command is executed
        private RelationshipRecord beforeUpdate;
        private RelationshipRecord before;

        public RelationshipCommand init( RelationshipRecord record )
        {
            setup( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            // the default (common) case is that the record to be written is complete and not from recovery or HA
            this.beforeUpdate = record;
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
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitRelationshipCommand( this );
        }

        public RelationshipRecord getRecord()
        {
            return record;
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeRelationshipFromCache( getKey() );
            if ( !record.inUse() )
            { // the relationship was deleted - invalidate the cached versions of the related nodes
                /*
                 * If isRecovered() then beforeUpdate is the correct one UNLESS this is the second time this command
                 * is executed, where it might have been actually written out to disk so the fields are already -1. So
                 * we still need to check.
                 * If !isRecovered() then beforeUpdate is the same as record, so we are still ok.
                 * The above is a hand waiving proof that the conditions that lead to the patchDeletedRelationshipNodes()
                 * in the if below are the same as in RelationshipCommand.execute() so it should be safe.
                 */
                if ( beforeUpdate.getFirstNode() != -1 || beforeUpdate.getSecondNode() != -1 )
                {
                    cacheAccess.patchDeletedRelationshipNodes( getKey(), beforeUpdate.getType(), beforeUpdate.getFirstNode(),
                            beforeUpdate.getFirstNextRel(), beforeUpdate.getSecondNode(), beforeUpdate.getSecondNextRel() );
                }
                if ( before != null )
                { // reading from the log
                    cacheAccess.removeNodeFromCache( before.getFirstNode() );
                    cacheAccess.removeNodeFromCache( before.getSecondNode() );
                }
                else
                { // applying from in-memory transaction state
                    cacheAccess.removeNodeFromCache( record.getFirstNode() );
                    cacheAccess.removeNodeFromCache( record.getSecondNode() );
                }
            }
        }

        public void setBefore( RelationshipRecord before )
        {
            this.before = before;
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
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitRelationshipGroupCommand( this );
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
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitNeoStoreCommand( this );
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        public NeoStoreRecord getRecord()
        {
            return record;
        }
    }

    public static class PropertyKeyTokenCommand extends Command
    {
        private PropertyKeyTokenRecord record;

        public PropertyKeyTokenCommand init( PropertyKeyTokenRecord record )
        {
            setup( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitPropertyKeyToken( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitPropertyKeyTokenCommand( this );
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        public PropertyKeyTokenRecord getRecord()
        {
            return record;
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
            return after.toString();
        }

        @Override
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitPropertyCommand( this );
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            long nodeId = this.getNodeId();
            long relId = this.getRelId();
            if ( nodeId != -1 )
            {
                cacheAccess.removeNodeFromCache( nodeId );
            }
            else if ( relId != -1 )
            {
                cacheAccess.removeRelationshipFromCache( relId );
            }
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

    public static class RelationshipTypeTokenCommand extends Command
    {
        private RelationshipTypeTokenRecord record;

        public RelationshipTypeTokenCommand init( RelationshipTypeTokenRecord record )
        {
            setup( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitRelationshipTypeToken( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitRelationshipTypeTokenCommand( this );
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        public RelationshipTypeTokenRecord getRecord()
        {
            return record;
        }
    }

    public static class LabelTokenCommand extends Command
    {
        private LabelTokenRecord record;

        public LabelTokenCommand init( LabelTokenRecord record )
        {
            setup( record.getId(), Mode.fromRecordState( record ) );
            this.record = record;
            return this;
        }

        @Override
        public void accept( CommandRecordVisitor visitor )
        {
            visitor.visitLabelToken( record );
        }

        @Override
        public String toString()
        {
            return record.toString();
        }

        @Override
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitLabelTokenCommand( this );
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            // no-op
        }

        public LabelTokenRecord getRecord()
        {
            return record;
        }
    }

    public static class SchemaRuleCommand extends Command
    {
        private Collection<DynamicRecord> recordsBefore;
        private Collection<DynamicRecord> recordsAfter;
        private SchemaRule schemaRule;

        private long txId;

        public SchemaRuleCommand init( Collection<DynamicRecord> recordsBefore,
                           Collection<DynamicRecord> recordsAfter, SchemaRule schemaRule, long txId )
        {
            setup( first( recordsAfter ).getId(), Mode.fromRecordState( first( recordsAfter ) ) );
            this.recordsBefore = recordsBefore;
            this.recordsAfter = recordsAfter;
            this.schemaRule = schemaRule;
            this.txId = txId;
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
        public boolean accept( NeoCommandVisitor visitor ) throws IOException
        {
            return visitor.visitSchemaRuleCommand( this );
        }

        @Override
        public void applyToCache( CacheAccessBackDoor cacheAccess )
        {
            cacheAccess.removeSchemaRuleFromCache( getKey() );
        }

        public Collection<DynamicRecord> getRecordsAfter()
        {
            return unmodifiableCollection( recordsAfter );
        }

        public SchemaRule getSchemaRule()
        {
            return schemaRule;
        }

        public long getTxId()
        {
            return txId;
        }

        public void setTxId( long txId )
        {
            this.txId = txId;
        }

        public Collection<DynamicRecord> getRecordsBefore()
        {
            return recordsBefore;
        }
    }
}
