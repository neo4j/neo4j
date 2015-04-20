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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;

public class NeoTransactionRollbackHandler extends NeoCommandHandler.Adapter
{
    private final boolean freeIds;

    public NeoTransactionRollbackHandler( boolean freeIds )
    {
        this.freeIds = freeIds;
    }

    /*
     *             for ( RecordProxy<Long, NodeRecord, Void> change : context.getNodeRecords().changes() )
            {
                NodeRecord record = change.forReadingLinkage();
                if ( freeIds && record.isCreated() )
                {
                    getNodeStore().freeId( record.getId() );
                }
                removeNodeFromCache( record.getId() );
            }
            for ( RecordChange<Long, RelationshipRecord, Void> change : context.getRelRecords().changes() )
            {
                long id = change.getKey();
                RelationshipRecord record = change.forReadingLinkage();
                if ( freeIds && change.isCreated() )
                {
                    getRelationshipStore().freeId( id );
                }
                removeRelationshipFromCache( id );
            }
            if ( neoStoreRecord != null )
            {
                removeGraphPropertiesFromCache();
            }

            rollbackTokenRecordChanges( context.getPropertyKeyTokenRecords(),
                    getPropertyKeyTokenStore(), freeIds, PROPERTY_KEY_CACHE_REMOVER );
            rollbackTokenRecordChanges( context.getLabelTokenRecords(), getLabelTokenStore(), freeIds,
                    LABEL_CACHE_REMOVER );
            rollbackTokenRecordChanges( context.getRelationshipTypeTokenRecords(), getRelationshipTypeStore(),
                    freeIds, RELATIONSHIP_TYPE_CACHE_REMOVER );

            for ( RecordProxy<Long, PropertyRecord, PrimitiveRecord> change : context.getPropertyRecords().changes() )
            {
                PropertyRecord record = change.forReadingLinkage();
                if ( record.getNodeId() != -1 )
                {
                    removeNodeFromCache( record.getNodeId() );
                }
                else if ( record.getRelId() != -1 )
                {
                    removeRelationshipFromCache( record.getRelId() );
                }
                if ( record.isCreated() )
                {
                    if ( freeIds )
                    {
                        getPropertyStore().freeId( record.getId() );
                    }
                    for ( PropertyBlock block : record.getPropertyBlocks() )
                    {
                        for ( DynamicRecord dynamicRecord : block.getValueRecords() )
                        {
                            if ( dynamicRecord.isCreated() )
                            {
                                if ( dynamicRecord.getType() == PropertyType.STRING.intValue() )
                                {
                                    getPropertyStore().freeStringBlockId(
                                            dynamicRecord.getId() );
                                }
                                else if ( dynamicRecord.getType() == PropertyType.ARRAY.intValue() )
                                {
                                    getPropertyStore().freeArrayBlockId(
                                            dynamicRecord.getId() );
                                }
                                else
                                {
                                    throw new InvalidRecordException(
                                            "Unknown type on " + dynamicRecord );
                                }
                            }
                        }
                    }
                }
            }
            for ( RecordProxy<Long, Collection<DynamicRecord>, SchemaRule> records : context.getSchemaRuleChanges().changes() )
            {
                long id = -1;
                for ( DynamicRecord record : records.forChangingData() )
                {
                    if ( id == -1 )
                    {
                        id = record.getId();
                    }
                    if ( freeIds && record.isCreated() )
                    {
                        getSchemaStore().freeId( record.getId() );
                    }
                }
            }
            for ( RecordProxy<Long, RelationshipGroupRecord, Integer> change : context.getRelGroupRecords().changes() )
            {
                RelationshipGroupRecord record = change.forReadingData();
                if ( freeIds && record.isCreated() )
                {
                    getRelationshipGroupStore().freeId( record.getId() );
                }
            }

    private interface CacheRemover
    {
        void remove( CacheAccessBackDoor cacheAccess, int id );
    }

    private static final CacheRemover PROPERTY_KEY_CACHE_REMOVER = new CacheRemover()
    {
        @Override
        public void remove( CacheAccessBackDoor cacheAccess, int id )
        {
            cacheAccess.removePropertyKeyFromCache( id );
        }
    };

    private static final CacheRemover RELATIONSHIP_TYPE_CACHE_REMOVER = new CacheRemover()
    {
        @Override
        public void remove( CacheAccessBackDoor cacheAccess, int id )
        {
            cacheAccess.removeRelationshipTypeFromCache( id );
        }
    };

    private static final CacheRemover LABEL_CACHE_REMOVER = new CacheRemover()
    {
        @Override
        public void remove( CacheAccessBackDoor cacheAccess, int id )
        {
            cacheAccess.removeLabelFromCache( id );
        }
    };

    private <T extends TokenRecord> void rollbackTokenRecordChanges( RecordChanges<Integer, T, Void> records,
            TokenStore<T> store, boolean freeIds, CacheRemover cacheRemover )
    {
        for ( RecordChange<Integer, T, Void> record : records.changes() )
        {
            if ( record.isCreated() )
            {
                if ( freeIds )
                {
                    store.freeId( record.getKey() );
                }
                for ( DynamicRecord dynamicRecord : record.forReadingLinkage().getNameRecords() )
                {
                    if ( dynamicRecord.isCreated() )
                    {
                        store.getNameStore().freeId( (int) dynamicRecord.getId() );
                    }
                }
            }
            cacheRemover.remove( cacheAccess, record.getKey() );
        }
    }

    private void removeRelationshipFromCache( long id )
    {
        cacheAccess.removeRelationshipFromCache( id );
    }

    private void removeNodeFromCache( long id )
    {
        cacheAccess.removeNodeFromCache( id );
    }

    private void removeGraphPropertiesFromCache()
    {
        cacheAccess.removeGraphPropertiesFromCache();
    }
     */
}
