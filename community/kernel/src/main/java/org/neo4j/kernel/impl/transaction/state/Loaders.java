/*
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.Loader;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class Loaders
{
    private final Loader<Long,NodeRecord,Void> nodeLoader;
    private final Loader<Long,PropertyRecord,PrimitiveRecord> propertyLoader;
    private final Loader<Long,RelationshipRecord,Void> relationshipLoader;
    private final Loader<Long,RelationshipGroupRecord,Integer> relationshipGroupLoader;
    private final Loader<Long,SchemaRecord,SchemaRule> schemaRuleLoader;
    private final Loader<Integer,PropertyKeyTokenRecord,Void> propertyKeyTokenLoader;
    private final Loader<Integer,LabelTokenRecord,Void> labelTokenLoader;
    private final Loader<Integer,RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader;

    public Loaders( NeoStores neoStores )
    {
        this(
                neoStores.getNodeStore(),
                neoStores.getPropertyStore(),
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                neoStores.getPropertyKeyTokenStore(),
                neoStores.getRelationshipTypeTokenStore(),
                neoStores.getLabelTokenStore(),
                neoStores.getSchemaStore() );
    }

    public Loaders(
            RecordStore<NodeRecord> nodeStore,
            PropertyStore propertyStore,
            RecordStore<RelationshipRecord> relationshipStore,
            RecordStore<RelationshipGroupRecord> relationshipGroupStore,
            RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore,
            RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore,
            RecordStore<LabelTokenRecord> labelTokenStore,
            SchemaStore schemaStore )
    {
        nodeLoader = nodeLoader( nodeStore );
        propertyLoader = propertyLoader( propertyStore );
        relationshipLoader = relationshipLoader( relationshipStore );
        relationshipGroupLoader = relationshipGroupLoader( relationshipGroupStore );
        schemaRuleLoader = schemaRuleLoader( schemaStore );
        propertyKeyTokenLoader = propertyKeyTokenLoader( propertyKeyTokenStore );
        labelTokenLoader = labelTokenLoader( labelTokenStore );
        relationshipTypeTokenLoader = relationshipTypeTokenLoader( relationshipTypeTokenStore );
    }

    public Loader<Long,NodeRecord,Void> nodeLoader()
    {
        return nodeLoader;
    }

    public Loader<Long,PropertyRecord,PrimitiveRecord> propertyLoader()
    {
        return propertyLoader;
    }

    public Loader<Long,RelationshipRecord,Void> relationshipLoader()
    {
        return relationshipLoader;
    }

    public Loader<Long,RelationshipGroupRecord,Integer> relationshipGroupLoader()
    {
        return relationshipGroupLoader;
    }

    public Loader<Long,SchemaRecord,SchemaRule> schemaRuleLoader()
    {
        return schemaRuleLoader;
    }

    public Loader<Integer,PropertyKeyTokenRecord,Void> propertyKeyTokenLoader()
    {
        return propertyKeyTokenLoader;
    }

    public Loader<Integer,LabelTokenRecord,Void> labelTokenLoader()
    {
        return labelTokenLoader;
    }

    public Loader<Integer,RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader()
    {
        return relationshipTypeTokenLoader;
    }

    public static Loader<Long,NodeRecord,Void> nodeLoader( final RecordStore<NodeRecord> store )
    {
        return new Loader<Long,NodeRecord,Void>()
        {
            @Override
            public NodeRecord newUnused( Long key, Void additionalData )
            {
                return andMarkAsCreated( new NodeRecord( key ) );
            }

            @Override
            public NodeRecord load( Long key, Void additionalData )
            {
                return store.getRecord( key, store.newRecord(), NORMAL );
            }

            @Override
            public void ensureHeavy( NodeRecord record )
            {
                store.ensureHeavy( record );
            }

            @Override
            public NodeRecord clone(NodeRecord nodeRecord)
            {
                return nodeRecord.clone();
            }
        };
    }

    public static Loader<Long,PropertyRecord,PrimitiveRecord> propertyLoader( final PropertyStore store )
    {
        return new Loader<Long,PropertyRecord,PrimitiveRecord>()
        {
            @Override
            public PropertyRecord newUnused( Long key, PrimitiveRecord additionalData )
            {
                PropertyRecord record = new PropertyRecord( key );
                setOwner( record, additionalData );
                return andMarkAsCreated( record );
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
                PropertyRecord record = store.getRecord( key, store.newRecord(), NORMAL );
                setOwner( record, additionalData );
                return record;
            }

            @Override
            public void ensureHeavy( PropertyRecord record )
            {
                for ( PropertyBlock block : record )
                {
                    store.ensureHeavy( block );
                }
            }

            @Override
            public PropertyRecord clone(PropertyRecord propertyRecord)
            {
                return propertyRecord.clone();
            }
        };
    }

    public static Loader<Long,RelationshipRecord,Void> relationshipLoader(
            final RecordStore<RelationshipRecord> store )
    {
        return new Loader<Long, RelationshipRecord, Void>()
        {
            @Override
            public RelationshipRecord newUnused( Long key, Void additionalData )
            {
                return andMarkAsCreated( new RelationshipRecord( key ) );
            }

            @Override
            public RelationshipRecord load( Long key, Void additionalData )
            {
                return store.getRecord( key, store.newRecord(), NORMAL );
            }

            @Override
            public void ensureHeavy( RelationshipRecord record )
            {   // Nothing to load
            }

            @Override
            public RelationshipRecord clone(RelationshipRecord relationshipRecord)
            {
                return relationshipRecord.clone();
            }
        };
    }

    public static Loader<Long,RelationshipGroupRecord,Integer> relationshipGroupLoader(
            final RecordStore<RelationshipGroupRecord> store )
    {
        return new Loader<Long, RelationshipGroupRecord, Integer>()
        {
            @Override
            public RelationshipGroupRecord newUnused( Long key, Integer type )
            {
                RelationshipGroupRecord record = new RelationshipGroupRecord( key );
                record.setType( type );
                return andMarkAsCreated( record );
            }

            @Override
            public RelationshipGroupRecord load( Long key, Integer type )
            {
                return store.getRecord( key, store.newRecord(), NORMAL );
            }

            @Override
            public void ensureHeavy( RelationshipGroupRecord record )
            {   // Not needed
            }

            @Override
            public RelationshipGroupRecord clone( RelationshipGroupRecord record )
            {
                return record.clone();
            }
        };
    }

    public static Loader<Long,SchemaRecord,SchemaRule> schemaRuleLoader( final SchemaStore store )
    {
        return new Loader<Long, SchemaRecord, SchemaRule>()
        {
            @Override
            public SchemaRecord newUnused( Long key, SchemaRule additionalData )
            {
                // Don't blindly mark as created here since some records may be reused.
                return new SchemaRecord( store.allocateFrom( additionalData ) );
            }

            @Override
            public SchemaRecord load( Long key, SchemaRule additionalData )
            {
                return new SchemaRecord( store.getRecords( key, RecordLoad.NORMAL ) );
            }

            @Override
            public void ensureHeavy( SchemaRecord records )
            {
                for ( DynamicRecord record : records)
                {
                    store.ensureHeavy(record);
                }
            }

            @Override
            public SchemaRecord clone( SchemaRecord records )
            {
                return records.clone();
            }
        };
    }

    public static Loader<Integer,PropertyKeyTokenRecord,Void> propertyKeyTokenLoader(
            final RecordStore<PropertyKeyTokenRecord> store )
    {
        return new Loader<Integer, PropertyKeyTokenRecord, Void>()
        {
            @Override
            public PropertyKeyTokenRecord newUnused( Integer key, Void additionalData )
            {
                return andMarkAsCreated( new PropertyKeyTokenRecord( key ) );
            }

            @Override
            public PropertyKeyTokenRecord load( Integer key, Void additionalData )
            {
                return store.getRecord( key, store.newRecord(), NORMAL );
            }

            @Override
            public void ensureHeavy( PropertyKeyTokenRecord record )
            {
                store.ensureHeavy( record );
            }

            @Override
            public PropertyKeyTokenRecord clone( PropertyKeyTokenRecord record )
            {
                return record.clone();
            }
        };
    }

    public static Loader<Integer,LabelTokenRecord,Void> labelTokenLoader(
            final RecordStore<LabelTokenRecord> store )
    {
        return new Loader<Integer, LabelTokenRecord, Void>()
        {
            @Override
            public LabelTokenRecord newUnused( Integer key, Void additionalData )
            {
                return andMarkAsCreated( new LabelTokenRecord( key ) );
            }

            @Override
            public LabelTokenRecord load( Integer key, Void additionalData )
            {
                return store.getRecord( key, store.newRecord(), NORMAL );
            }

            @Override
            public void ensureHeavy( LabelTokenRecord record )
            {
                store.ensureHeavy( record );
            }

            @Override
            public LabelTokenRecord clone( LabelTokenRecord record )
            {
                return record.clone();
            }
        };
    }

    public static Loader<Integer,RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader(
            final RecordStore<RelationshipTypeTokenRecord> store )
    {
        return new Loader<Integer, RelationshipTypeTokenRecord, Void>()
        {
            @Override
            public RelationshipTypeTokenRecord newUnused( Integer key, Void additionalData )
            {
                return andMarkAsCreated( new RelationshipTypeTokenRecord( key ) );
            }

            @Override
            public RelationshipTypeTokenRecord load( Integer key, Void additionalData )
            {
                return store.getRecord( key, store.newRecord(), NORMAL );
            }

            @Override
            public void ensureHeavy( RelationshipTypeTokenRecord record )
            {
                store.ensureHeavy( record );
            }

            @Override
            public RelationshipTypeTokenRecord clone( RelationshipTypeTokenRecord record )
            {
                return record.clone();
            }
        };
    }

    protected static <RECORD extends AbstractBaseRecord> RECORD andMarkAsCreated( RECORD record )
    {
        record.setCreated();
        return record;
    }
}
