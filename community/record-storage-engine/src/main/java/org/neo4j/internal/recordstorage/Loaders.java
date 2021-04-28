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

import org.neo4j.internal.recordstorage.RecordAccess.Loader;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static java.lang.Math.toIntExact;

public class Loaders implements AutoCloseable
{
    private final RecordLoader<NodeRecord,Void> nodeLoader;
    private final RecordLoader<PropertyRecord,PrimitiveRecord> propertyLoader;
    private final RecordLoader<RelationshipRecord,Void> relationshipLoader;
    private final RecordLoader<RelationshipGroupRecord,Integer> relationshipGroupLoader;
    private final RecordLoader<SchemaRecord, SchemaRule> schemaRuleLoader;
    private final RecordLoader<PropertyKeyTokenRecord,Void> propertyKeyTokenLoader;
    private final RecordLoader<LabelTokenRecord,Void> labelTokenLoader;
    private final RecordLoader<RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader;

    public Loaders( NeoStores neoStores, CursorContext cursorContext )
    {
        this(
                neoStores.getNodeStore(),
                neoStores.getPropertyStore(),
                neoStores.getRelationshipStore(),
                neoStores.getRelationshipGroupStore(),
                neoStores.getPropertyKeyTokenStore(),
                neoStores.getRelationshipTypeTokenStore(),
                neoStores.getLabelTokenStore(),
                neoStores.getSchemaStore(),
                cursorContext );
    }

    public Loaders(
            RecordStore<NodeRecord> nodeStore,
            PropertyStore propertyStore,
            RecordStore<RelationshipRecord> relationshipStore,
            RecordStore<RelationshipGroupRecord> relationshipGroupStore,
            RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore,
            RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore,
            RecordStore<LabelTokenRecord> labelTokenStore,
            SchemaStore schemaStore,
            CursorContext cursorContext )
    {
        nodeLoader = nodeLoader( nodeStore, cursorContext );
        propertyLoader = propertyLoader( propertyStore, cursorContext );
        relationshipLoader = relationshipLoader( relationshipStore, cursorContext );
        relationshipGroupLoader = relationshipGroupLoader( relationshipGroupStore, cursorContext );
        schemaRuleLoader = schemaRuleLoader( schemaStore, cursorContext );
        propertyKeyTokenLoader = propertyKeyTokenLoader( propertyKeyTokenStore, cursorContext );
        labelTokenLoader = labelTokenLoader( labelTokenStore, cursorContext );
        relationshipTypeTokenLoader = relationshipTypeTokenLoader( relationshipTypeTokenStore, cursorContext );
    }

    @Override
    public void close()
    {
        IOUtils.closeAllUnchecked( nodeLoader, propertyLoader, relationshipLoader, relationshipGroupLoader, schemaRuleLoader, propertyKeyTokenLoader,
                labelTokenLoader, relationshipTypeTokenLoader );
    }

    public Loader<NodeRecord,Void> nodeLoader()
    {
        return nodeLoader;
    }

    public Loader<PropertyRecord,PrimitiveRecord> propertyLoader()
    {
        return propertyLoader;
    }

    public Loader<RelationshipRecord,Void> relationshipLoader()
    {
        return relationshipLoader;
    }

    public Loader<RelationshipGroupRecord,Integer> relationshipGroupLoader()
    {
        return relationshipGroupLoader;
    }

    public Loader<SchemaRecord,SchemaRule> schemaRuleLoader()
    {
        return schemaRuleLoader;
    }

    public Loader<PropertyKeyTokenRecord,Void> propertyKeyTokenLoader()
    {
        return propertyKeyTokenLoader;
    }

    public Loader<LabelTokenRecord,Void> labelTokenLoader()
    {
        return labelTokenLoader;
    }

    public Loader<RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader()
    {
        return relationshipTypeTokenLoader;
    }

    public static RecordLoader<NodeRecord,Void> nodeLoader( final RecordStore<NodeRecord> store, CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public NodeRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new NodeRecord( key ) );
            }

            @Override
            public NodeRecord copy( NodeRecord nodeRecord )
            {
                return new NodeRecord( nodeRecord );
            }
        };
    }

    public static RecordLoader<PropertyRecord,PrimitiveRecord> propertyLoader( final PropertyStore store, CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public PropertyRecord newUnused( long key, PrimitiveRecord additionalData )
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
            public PropertyRecord load( long key, PrimitiveRecord additionalData, RecordLoad load, CursorContext cursorContext )
            {
                PropertyRecord record = super.load( key, additionalData, load, cursorContext );
                setOwner( record, additionalData );
                return record;
            }

            @Override
            public PropertyRecord copy( PropertyRecord propertyRecord )
            {
                return new PropertyRecord( propertyRecord );
            }
        };
    }

    public static RecordLoader<RelationshipRecord,Void> relationshipLoader( final RecordStore<RelationshipRecord> store, CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public RelationshipRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new RelationshipRecord( key ) );
            }

            @Override
            public RelationshipRecord copy( RelationshipRecord relationshipRecord )
            {
                return new RelationshipRecord( relationshipRecord );
            }
        };
    }

    public static RecordLoader<RelationshipGroupRecord,Integer> relationshipGroupLoader( final RecordStore<RelationshipGroupRecord> store,
            CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public RelationshipGroupRecord newUnused( long key, Integer type )
            {
                RelationshipGroupRecord record = new RelationshipGroupRecord( key );
                record.setType( type );
                return andMarkAsCreated( record );
            }

            @Override
            public RelationshipGroupRecord copy( RelationshipGroupRecord record )
            {
                return new RelationshipGroupRecord( record );
            }
        };
    }

    private static RecordLoader<SchemaRecord, SchemaRule> schemaRuleLoader( final SchemaStore store, CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public SchemaRecord newUnused( long key, SchemaRule additionalData )
            {
                return andMarkAsCreated( new SchemaRecord( key ) );
            }

            @Override
            public SchemaRecord copy( SchemaRecord record )
            {
                return new SchemaRecord( record );
            }
        };
    }

    public static RecordLoader<PropertyKeyTokenRecord,Void> propertyKeyTokenLoader( final RecordStore<PropertyKeyTokenRecord> store,
            CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public PropertyKeyTokenRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new PropertyKeyTokenRecord( toIntExact( key ) ) );
            }

            @Override
            public PropertyKeyTokenRecord copy( PropertyKeyTokenRecord record )
            {
                return new PropertyKeyTokenRecord( record );
            }
        };
    }

    public static RecordLoader<LabelTokenRecord,Void> labelTokenLoader( final RecordStore<LabelTokenRecord> store, CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public LabelTokenRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new LabelTokenRecord( toIntExact( key ) ) );
            }

            @Override
            public LabelTokenRecord copy( LabelTokenRecord record )
            {
                return new LabelTokenRecord( record );
            }
        };
    }

    public static RecordLoader<RelationshipTypeTokenRecord,Void> relationshipTypeTokenLoader( final RecordStore<RelationshipTypeTokenRecord> store,
            CursorContext cursorContext )
    {
        return new RecordLoader<>( store, cursorContext )
        {
            @Override
            public RelationshipTypeTokenRecord newUnused( long key, Void additionalData )
            {
                return andMarkAsCreated( new RelationshipTypeTokenRecord( toIntExact( key ) ) );
            }

            @Override
            public RelationshipTypeTokenRecord copy( RelationshipTypeTokenRecord record )
            {
                return new RelationshipTypeTokenRecord( record );
            }
        };
    }

    protected static <RECORD extends AbstractBaseRecord> RECORD andMarkAsCreated( RECORD record )
    {
        record.setCreated();
        return record;
    }

    private abstract static class RecordLoader<R extends AbstractBaseRecord,A> implements Loader<R,A>, AutoCloseable
    {
        private final RecordStore<R> store;
        private final PageCursor cursor;

        RecordLoader( RecordStore<R> store, CursorContext cursorContext )
        {
            this.store = store;
            this.cursor = store.openPageCursorForReading( 0, cursorContext );
        }

        @Override
        public void ensureHeavy( R record, CursorContext cursorContext )
        {
            store.ensureHeavy( record, cursorContext );
        }

        @Override
        public R load( long key, A additionalData, RecordLoad load, CursorContext cursorContext )
        {
            R record = store.newRecord();
            store.getRecordByCursor( key, record, load, cursor );
            return record;
        }

        @Override
        public void close()
        {
            cursor.close();
        }
    }
}
