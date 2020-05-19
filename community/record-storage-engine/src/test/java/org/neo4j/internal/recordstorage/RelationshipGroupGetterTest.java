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

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RelationshipGroupGetter.RelationshipGroupPosition;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class RelationshipGroupGetterTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldAbortLoadingGroupChainIfComeTooFar()
    {
        // GIVEN a node with relationship group chain 2-->4-->10-->23
        LogProvider logProvider = NullLogProvider.getInstance();
        StoreFactory storeFactory = new StoreFactory( databaseLayout, Config.defaults(),
                new DefaultIdGeneratorFactory( fs, immediate()  ), pageCache, fs, logProvider, PageCacheTracer.NULL );
        try ( NeoStores stores = storeFactory.openNeoStores( true, StoreType.RELATIONSHIP_GROUP ) )
        {
            RecordStore<RelationshipGroupRecord> store = spy( stores.getRelationshipGroupStore() );

            RelationshipGroupRecord group2 = group( 0, 2 );
            RelationshipGroupRecord group4 = group( 1, 4 );
            RelationshipGroupRecord group10 = group( 2, 10 );
            RelationshipGroupRecord group23 = group( 3, 23 );
            link( group2, group4, group10, group23 );
            store.updateRecord( group2, NULL );
            store.updateRecord( group4, NULL );
            store.updateRecord( group10, NULL );
            store.updateRecord( group23, NULL );
            RelationshipGroupGetter groupGetter = new RelationshipGroupGetter( store, NULL );
            NodeRecord node = new NodeRecord( 0 ).initialize( false, 01, true, group2.getId(), 0 );

            // WHEN trying to find relationship group 7
            RecordAccess<RelationshipGroupRecord, Integer> access =
                    new DirectRecordAccess<>( store, Loaders.relationshipGroupLoader( store ) );
            RelationshipGroupPosition result = groupGetter.getRelationshipGroup( node, 7, access );

            // THEN only groups 2, 4 and 10 should have been loaded
            InOrder verification = inOrder( store );
            verification.verify( store ).getRecord( eq( group2.getId() ), any( RelationshipGroupRecord.class ), any( RecordLoad.class ), any() );
            verification.verify( store ).getRecord( eq( group4.getId() ), any( RelationshipGroupRecord.class ), any( RecordLoad.class ), any() );
            verification.verify( store ).getRecord( eq( group10.getId() ), any( RelationshipGroupRecord.class ), any( RecordLoad.class ), any() );
            verification.verify( store, never() )
                    .getRecord( eq( group23.getId() ), any( RelationshipGroupRecord.class ), any( RecordLoad.class ), any() );

            // it should also be reported as not found
            assertNull( result.group() );
            // with group 4 as closes previous one
            assertEquals( group4, result.closestPrevious().forReadingData() );
        }
    }

    private static void link( RelationshipGroupRecord... groups )
    {
        for ( int i = 0; i < groups.length; i++ )
        {
            if ( i > 0 )
            {
                groups[i].setPrev( groups[i - 1].getId() );
            }
            if ( i < groups.length - 1 )
            {
                groups[i].setNext( groups[i + 1].getId() );
            }
        }
    }

    private static RelationshipGroupRecord group( long id, int type )
    {
        RelationshipGroupRecord group = new RelationshipGroupRecord( id );
        group.setType( type );
        group.setInUse( true );
        return group;
    }
}
