/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.Loader;

public class Loaders
{
    public static Loader<Long,NodeRecord,Void> nodeLoader( final NodeStore store )
    {
        return new Loader<Long,NodeRecord,Void>()
        {
            @Override
            public NodeRecord newUnused( Long key, Void additionalData )
            {
                return andMarkAsCreated( new NodeRecord( key, false, Record.NO_NEXT_RELATIONSHIP.intValue(),
                        Record.NO_NEXT_PROPERTY.intValue() ) );
            }

            @Override
            public NodeRecord load( Long key, Void additionalData )
            {
                return store.getRecord( key );
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
                PropertyRecord record = store.getRecord( key.longValue() );
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

    public static Loader<Long,RelationshipRecord,Void> relationshipLoader( final RelationshipStore store )
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
                return store.getRecord( key );
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
        };
    }

    public static Loader<Long,RelationshipGroupRecord,Integer> relationshipGroupLoader(
            final RelationshipGroupStore store )
    {
        return new Loader<Long, RelationshipGroupRecord, Integer>()
        {
            @Override
            public RelationshipGroupRecord newUnused( Long key, Integer type )
            {
                return andMarkAsCreated( new RelationshipGroupRecord( key, type ) );
            }

            @Override
            public RelationshipGroupRecord load( Long key, Integer type )
            {
                return store.getRecord( key );
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
        };
    }

    public static Loader<Long,Collection<DynamicRecord>,SchemaRule> schemaRuleLoader( final SchemaStore store )
    {
        return new Loader<Long, Collection<DynamicRecord>, SchemaRule>()
        {
            @Override
            public Collection<DynamicRecord> newUnused(Long key, SchemaRule additionalData )
            {
                // Don't blindly mark as created here since some records may be reused.
                return store.allocateFrom( additionalData );
            }

            @Override
            public Collection<DynamicRecord> load(Long key, SchemaRule additionalData )
            {
                return store.getRecords( key );
            }

            @Override
            public void ensureHeavy(Collection<DynamicRecord> dynamicRecords )
            {
                for ( DynamicRecord record : dynamicRecords)
                {
                    store.ensureHeavy(record);
                }
            }

            @Override
            public Collection<DynamicRecord> clone( Collection<DynamicRecord> dynamicRecords )
            {
                Collection<DynamicRecord> list = new ArrayList<>( dynamicRecords.size() );
                for ( DynamicRecord record : dynamicRecords )
                {
                    list.add( record.clone() );
                }
                return list;
            }
        };
    }

    public static Loader<Integer,PropertyKeyTokenRecord,Void> propertyKeyTokenLoader(
            final TokenStore<PropertyKeyTokenRecord, Token> store )
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
                return store.getRecord( key );
            }

            @Override
            public void ensureHeavy( PropertyKeyTokenRecord record )
            {
                store.ensureHeavy( record );
            }

            @Override
            public PropertyKeyTokenRecord clone( PropertyKeyTokenRecord record )
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static Loader<Integer,LabelTokenRecord,Void> labelTokenLoader(
            final TokenStore<LabelTokenRecord, Token> store )
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
                return store.getRecord( key );
            }

            @Override
            public void ensureHeavy( LabelTokenRecord record )
            {
                store.ensureHeavy( record );
            }

            @Override
            public LabelTokenRecord clone( LabelTokenRecord record )
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static Loader<Integer,RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader(
            final TokenStore<RelationshipTypeTokenRecord, RelationshipTypeToken> store )
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
                return store.getRecord( key );
            }

            @Override
            public void ensureHeavy( RelationshipTypeTokenRecord record )
            {
                store.ensureHeavy( record );
            }

            @Override
            public RelationshipTypeTokenRecord clone( RelationshipTypeTokenRecord record )
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected static <RECORD extends AbstractBaseRecord> RECORD andMarkAsCreated( RECORD record )
    {
        record.setCreated();
        return record;
    }
}
