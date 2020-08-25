/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static java.lang.Math.toIntExact;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class Loaders
{
    private final Loader<NodeRecord> nodeLoader;
    private final Loader<PropertyRecord> propertyLoader;
    private final Loader<RelationshipRecord> relationshipLoader;
    private final Loader<RelationshipGroupRecord> relationshipGroupLoader;
    private final Loader<SchemaRecord> schemaRuleLoader;
    private final Loader<PropertyKeyTokenRecord> propertyKeyTokenLoader;
    private final Loader<LabelTokenRecord> labelTokenLoader;
    private final Loader<RelationshipTypeTokenRecord> relationshipTypeTokenLoader;

    public Loaders( NeoStores neoStores )
    {
        nodeLoader = nodeLoader( neoStores.getNodeStore() );
        propertyLoader = propertyLoader( neoStores.getPropertyStore() );
        relationshipLoader = relationshipLoader( neoStores.getRelationshipStore() );
        relationshipGroupLoader = relationshipGroupLoader( neoStores.getRelationshipGroupStore() );
        schemaRuleLoader = schemaRuleLoader( neoStores.getSchemaStore() );
        propertyKeyTokenLoader = propertyKeyTokenLoader( neoStores.getPropertyKeyTokenStore() );
        labelTokenLoader = labelTokenLoader( neoStores.getLabelTokenStore() );
        relationshipTypeTokenLoader = relationshipTypeTokenLoader( neoStores.getRelationshipTypeTokenStore() );
    }

    public Loader<NodeRecord> nodeLoader()
    {
        return nodeLoader;
    }

    public Loader<PropertyRecord> propertyLoader()
    {
        return propertyLoader;
    }

    public Loader<RelationshipRecord> relationshipLoader()
    {
        return relationshipLoader;
    }

    public Loader<RelationshipGroupRecord> relationshipGroupLoader()
    {
        return relationshipGroupLoader;
    }

    public Loader<SchemaRecord> schemaRuleLoader()
    {
        return schemaRuleLoader;
    }

    public Loader<PropertyKeyTokenRecord> propertyKeyTokenLoader()
    {
        return propertyKeyTokenLoader;
    }

    public Loader<LabelTokenRecord> labelTokenLoader()
    {
        return labelTokenLoader;
    }

    public Loader<RelationshipTypeTokenRecord> relationshipTypeTokenLoader()
    {
        return relationshipTypeTokenLoader;
    }

    public static Loader<NodeRecord> nodeLoader( final RecordStore<NodeRecord> store )
    {
        return new NodeRecordLoader( store );
    }

    public static Loader<PropertyRecord> propertyLoader( final PropertyStore store )
    {
        return new PropertyRecordLoader( store );
    }

    public static Loader<RelationshipRecord> relationshipLoader( final RecordStore<RelationshipRecord> store )
    {
        return new RelationshipRecordLoader( store );
    }

    public static Loader<RelationshipGroupRecord> relationshipGroupLoader( final RecordStore<RelationshipGroupRecord> store )
    {
        return new RelationshipGroupRecordLoader( store );
    }

    private static Loader<SchemaRecord> schemaRuleLoader( final SchemaStore store )
    {
        return new SchemaRecordLoader( store );
    }

    public static Loader<PropertyKeyTokenRecord> propertyKeyTokenLoader( final RecordStore<PropertyKeyTokenRecord> store )
    {
        return new PropertyKeyTokenRecordLoader( store );
    }

    public static Loader<LabelTokenRecord> labelTokenLoader( final RecordStore<LabelTokenRecord> store )
    {
        return new LabelTokenRecordLoader( store );
    }

    public static Loader<RelationshipTypeTokenRecord> relationshipTypeTokenLoader( final RecordStore<RelationshipTypeTokenRecord> store )
    {
        return new RelationshipTypeTokenRecordLoader( store );
    }

    protected static <RECORD extends AbstractBaseRecord> RECORD andMarkAsCreated( RECORD record )
    {
        record.setCreated();
        return record;
    }

    private static class RelationshipTypeTokenRecordLoader implements Loader<RelationshipTypeTokenRecord>
    {
        private final RecordStore<RelationshipTypeTokenRecord> store;

        RelationshipTypeTokenRecordLoader( RecordStore<RelationshipTypeTokenRecord> store )
        {
            this.store = store;
        }

        @Override
        public RelationshipTypeTokenRecord newUnused( long key )
        {
            return andMarkAsCreated( new RelationshipTypeTokenRecord( toIntExact( key ) ) );
        }

        @Override
        public RelationshipTypeTokenRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( RelationshipTypeTokenRecord record, PageCursorTracer cursorTracer )
        {
            store.ensureHeavy( record, cursorTracer );
        }

        @Override
        public RelationshipTypeTokenRecord copy( RelationshipTypeTokenRecord record )
        {
            return new RelationshipTypeTokenRecord( record );
        }
    }

    private static class LabelTokenRecordLoader implements Loader<LabelTokenRecord>
    {
        private final RecordStore<LabelTokenRecord> store;

        LabelTokenRecordLoader( RecordStore<LabelTokenRecord> store )
        {
            this.store = store;
        }

        @Override
        public LabelTokenRecord newUnused( long key )
        {
            return andMarkAsCreated( new LabelTokenRecord( toIntExact( key ) ) );
        }

        @Override
        public LabelTokenRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( LabelTokenRecord record, PageCursorTracer cursorTracer )
        {
            store.ensureHeavy( record, cursorTracer );
        }

        @Override
        public LabelTokenRecord copy( LabelTokenRecord record )
        {
            return new LabelTokenRecord( record );
        }
    }

    private static class PropertyKeyTokenRecordLoader implements Loader<PropertyKeyTokenRecord>
    {
        private final RecordStore<PropertyKeyTokenRecord> store;

        PropertyKeyTokenRecordLoader( RecordStore<PropertyKeyTokenRecord> store )
        {
            this.store = store;
        }

        @Override
        public PropertyKeyTokenRecord newUnused( long key )
        {
            return andMarkAsCreated( new PropertyKeyTokenRecord( toIntExact( key ) ) );
        }

        @Override
        public PropertyKeyTokenRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( PropertyKeyTokenRecord record, PageCursorTracer cursorTracer )
        {
            store.ensureHeavy( record, cursorTracer );
        }

        @Override
        public PropertyKeyTokenRecord copy( PropertyKeyTokenRecord record )
        {
            return new PropertyKeyTokenRecord( record );
        }
    }

    private static class SchemaRecordLoader implements Loader<SchemaRecord>
    {
        private final SchemaStore store;

        SchemaRecordLoader( SchemaStore store )
        {
            this.store = store;
        }

        @Override
        public SchemaRecord newUnused( long key )
        {
            return andMarkAsCreated( new SchemaRecord( key ) );
        }

        @Override
        public SchemaRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), RecordLoad.NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( SchemaRecord record, PageCursorTracer cursorTracer )
        {
        }

        @Override
        public SchemaRecord copy( SchemaRecord record )
        {
            return new SchemaRecord( record );
        }
    }

    private static class RelationshipGroupRecordLoader implements Loader<RelationshipGroupRecord>
    {
        private final RecordStore<RelationshipGroupRecord> store;

        RelationshipGroupRecordLoader( RecordStore<RelationshipGroupRecord> store )
        {
            this.store = store;
        }

        @Override
        public RelationshipGroupRecord newUnused( long key )
        {
            RelationshipGroupRecord record = new RelationshipGroupRecord( key );
            return andMarkAsCreated( record );
        }

        @Override
        public RelationshipGroupRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( RelationshipGroupRecord record, PageCursorTracer cursorTracer )
        {   // Not needed
        }

        @Override
        public RelationshipGroupRecord copy( RelationshipGroupRecord record )
        {
            return new RelationshipGroupRecord( record );
        }
    }

    private static class RelationshipRecordLoader implements Loader<RelationshipRecord>
    {
        private final RecordStore<RelationshipRecord> store;

        RelationshipRecordLoader( RecordStore<RelationshipRecord> store )
        {
            this.store = store;
        }

        @Override
        public RelationshipRecord newUnused( long key )
        {
            return andMarkAsCreated( new RelationshipRecord( key ) );
        }

        @Override
        public RelationshipRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( RelationshipRecord record, PageCursorTracer cursorTracer )
        {   // Nothing to load
        }

        @Override
        public RelationshipRecord copy( RelationshipRecord relationshipRecord )
        {
            return new RelationshipRecord( relationshipRecord );
        }
    }

    private static class NodeRecordLoader implements Loader<NodeRecord>
    {
        private final RecordStore<NodeRecord> store;

        NodeRecordLoader( RecordStore<NodeRecord> store )
        {
            this.store = store;
        }

        @Override
        public NodeRecord newUnused( long key )
        {
            return andMarkAsCreated( new NodeRecord( key ) );
        }

        @Override
        public NodeRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( NodeRecord record, PageCursorTracer cursorTracer )
        {
            store.ensureHeavy( record, cursorTracer );
        }

        @Override
        public NodeRecord copy( NodeRecord nodeRecord )
        {
            return new NodeRecord( nodeRecord );
        }
    }

    private static class PropertyRecordLoader implements Loader<PropertyRecord>
    {
        private final PropertyStore store;

        PropertyRecordLoader( PropertyStore store )
        {
            this.store = store;
        }

        @Override
        public PropertyRecord newUnused( long key )
        {
            PropertyRecord record = new PropertyRecord( key );
            return andMarkAsCreated( record );
        }

        @Override
        public PropertyRecord load( long key, PageCursorTracer cursorTracer )
        {
            return store.getRecord( key, store.newRecord(), NORMAL, cursorTracer );
        }

        @Override
        public void ensureHeavy( PropertyRecord record, PageCursorTracer cursorTracer )
        {
            for ( PropertyBlock block : record )
            {
                store.ensureHeavy( block, cursorTracer );
            }
        }

        @Override
        public PropertyRecord copy( PropertyRecord propertyRecord )
        {
            return new PropertyRecord( propertyRecord );
        }
    }
}
