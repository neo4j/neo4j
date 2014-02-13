/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;

public class RecordChangeSet
{
    private final NeoStore neoStore;

    private final RecordChanges<Long, NodeRecord, Void> nodeRecords =
            new RecordChanges<>( new RecordChanges.Loader<Long, NodeRecord, Void>()
            {
                @Override
                public NodeRecord newUnused( Long key, Void additionalData )
                {
                    return new NodeRecord( key, false, Record.NO_NEXT_RELATIONSHIP.intValue(),
                            Record.NO_NEXT_PROPERTY.intValue() );
                }

                @Override
                public NodeRecord load( Long key, Void additionalData )
                {
                    return neoStore.getNodeStore().getRecord( key );
                }

                @Override
                public void ensureHeavy( NodeRecord record )
                {
                    neoStore.getNodeStore().ensureHeavy( record );
                }

                @Override
                public NodeRecord clone(NodeRecord nodeRecord)
                {
                    return nodeRecord.clone();
                }
            }, true );
    private final RecordChanges<Long, PropertyRecord, PrimitiveRecord> propertyRecords =
            new RecordChanges<>( new RecordChanges.Loader<Long, PropertyRecord, PrimitiveRecord>()
            {
                @Override
                public PropertyRecord newUnused( Long key, PrimitiveRecord additionalData )
                {
                    PropertyRecord record = new PropertyRecord( key );
                    setOwner( record, additionalData );
                    return record;
                }

                private void setOwner( PropertyRecord record, PrimitiveRecord owner )
                {
                    if ( owner != null )
                    {
                        owner.setIdTo( record );
                    }
                }

                @Override
                public PropertyRecord load( Long key, PrimitiveRecord additionalData )
                {
                    PropertyRecord record = neoStore.getPropertyStore().getRecord( key.longValue() );
                    setOwner( record, additionalData );
                    return record;
                }

                @Override
                public void ensureHeavy( PropertyRecord record )
                {
                    for ( PropertyBlock block : record.getPropertyBlocks() )
                    {
                        neoStore.getPropertyStore().ensureHeavy( block );
                    }
                }

                @Override
                public PropertyRecord clone(PropertyRecord propertyRecord)
                {
                    return propertyRecord.clone();
                }
            }, true );
    private final RecordChanges<Long, RelationshipRecord, Void> relRecords =
            new RecordChanges<>( new RecordChanges.Loader<Long, RelationshipRecord, Void>()
            {
                @Override
                public RelationshipRecord newUnused( Long key, Void additionalData )
                {
                    return new RelationshipRecord( key );
                }

                @Override
                public RelationshipRecord load( Long key, Void additionalData )
                {
                    return neoStore.getRelationshipStore().getRecord( key );
                }

                @Override
                public void ensureHeavy( RelationshipRecord record )
                {
                }

                @Override
                public RelationshipRecord clone(RelationshipRecord relationshipRecord) {
                    // Not needed because we don't manage before state for relationship records.
                    throw new UnsupportedOperationException("Unexpected call to clone on a relationshipRecord");
                }
            }, false );
    private final RecordChanges<Long, RelationshipGroupRecord, Integer> relGroupRecords =
            new RecordChanges<>( new RecordChanges.Loader<Long, RelationshipGroupRecord, Integer>()
            {
                @Override
                public RelationshipGroupRecord newUnused( Long key, Integer type )
                {
                    return new RelationshipGroupRecord( key, type );
                }

                @Override
                public RelationshipGroupRecord load( Long key, Integer type )
                {
                    return neoStore.getRelationshipGroupStore().getRecord( key );
                }

                @Override
                public void ensureHeavy( RelationshipGroupRecord record )
                {   // Not needed
                }

                @Override
                public RelationshipGroupRecord clone( RelationshipGroupRecord record )
                {
                    throw new UnsupportedOperationException();
                }
            }, false );
    private final RecordChanges<Long, Collection<DynamicRecord>, SchemaRule> schemaRuleChanges =
            new RecordChanges<>(new RecordChanges.Loader<Long, Collection<DynamicRecord>, SchemaRule>()
            {
                @Override
                public Collection<DynamicRecord> newUnused(Long key, SchemaRule additionalData)
                {
                    return neoStore.getSchemaStore().allocateFrom(additionalData);
                }

                @Override
                public Collection<DynamicRecord> load(Long key, SchemaRule additionalData)
                {
                    return neoStore.getSchemaStore().getRecords( key );
                }

                @Override
                public void ensureHeavy(Collection<DynamicRecord> dynamicRecords)
                {
                    SchemaStore schemaStore = neoStore.getSchemaStore();
                    for ( DynamicRecord record : dynamicRecords)
                    {
                        schemaStore.ensureHeavy(record);
                    }
                }

                @Override
                public Collection<DynamicRecord> clone(Collection<DynamicRecord> dynamicRecords) {
                    Collection<DynamicRecord> list = new ArrayList<>( dynamicRecords.size() );
                    for ( DynamicRecord record : dynamicRecords)
                    {
                        list.add( record.clone() );
                    }
                    return list;
                }
            }, true);

    private final Map<Long, Command.NodeCommand> nodeCommands = new TreeMap<>();
    private final ArrayList<Command.PropertyCommand> propCommands = new ArrayList<>();
    private final ArrayList<Command.RelationshipCommand> relCommands = new ArrayList<>();
    private final ArrayList<Command.SchemaRuleCommand> schemaRuleCommands = new ArrayList<>();
    private final ArrayList<Command.RelationshipGroupCommand> relGroupCommands = new ArrayList<>();
    private final ArrayList<Command.RelationshipTypeTokenCommand> relationshipTypeTokenCommands = new ArrayList<>();
    private final ArrayList<Command.LabelTokenCommand> labelTokenCommands = new ArrayList<>();
    private final ArrayList<Command.PropertyKeyTokenCommand> propertyKeyTokenCommands = new ArrayList<>();
    private Command.NeoStoreCommand neoStoreCommand;

    public RecordChangeSet( NeoStore neoStore )
    {
        this.neoStore = neoStore;
    }

    public RecordChanges<Long, NodeRecord, Void> getNodeRecords()
    {
        return nodeRecords;
    }

    public RecordChanges<Long, PropertyRecord, PrimitiveRecord> getPropertyRecords()
    {
        return propertyRecords;
    }

    public RecordChanges<Long, RelationshipRecord, Void> getRelRecords()
    {
        return relRecords;
    }

    public RecordChanges<Long, RelationshipGroupRecord, Integer> getRelGroupRecords()
    {
        return relGroupRecords;
    }

    public RecordChanges<Long, Collection<DynamicRecord>, SchemaRule> getSchemaRuleChanges()
    {
        return schemaRuleChanges;
    }

    public Map<Long, Command.NodeCommand> getNodeCommands()
    {
        return nodeCommands;
    }

    public ArrayList<Command.PropertyCommand> getPropCommands()
    {
        return propCommands;
    }

    public ArrayList<Command.RelationshipCommand> getRelCommands()
    {
        return relCommands;
    }

    public ArrayList<Command.SchemaRuleCommand> getSchemaRuleCommands()
    {
        return schemaRuleCommands;
    }

    public ArrayList<Command.RelationshipGroupCommand> getRelGroupCommands()
    {
        return relGroupCommands;
    }

    public ArrayList<Command.RelationshipTypeTokenCommand> getRelationshipTypeTokenCommands()
    {
        return relationshipTypeTokenCommands;
    }

    public ArrayList<Command.LabelTokenCommand> getLabelTokenCommands()
    {
        return labelTokenCommands;
    }

    public ArrayList<Command.PropertyKeyTokenCommand> getPropertyKeyTokenCommands()
    {
        return propertyKeyTokenCommands;
    }

    public Command.NeoStoreCommand getNeoStoreCommand()
    {
        return neoStoreCommand;
    }

    public void generateNeoStoreCommand( NeoStoreRecord neoStoreRecord )
    {
        neoStoreCommand = new Command.NeoStoreCommand( neoStore, neoStoreRecord );
    }

    public void setNeoStoreCommand( Command.NeoStoreCommand command )
    {
        neoStoreCommand = command;
    }

    public void close()
    {
        nodeRecords.clear();
        propertyRecords.clear();
        relRecords.clear();
        schemaRuleChanges.clear();
        relGroupRecords.clear();

        nodeCommands.clear();
        propCommands.clear();
        propertyKeyTokenCommands.clear();
        relCommands.clear();
        schemaRuleCommands.clear();
        relationshipTypeTokenCommands.clear();
        labelTokenCommands.clear();
        relGroupCommands.clear();
        neoStoreCommand = null;
    }
}
