/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class HighIdTransactionApplierTest
{
    private static final IndexProviderDescriptor PROVIDER_DESCRIPTOR = new IndexProviderDescriptor( "empty", "1" );

    @Inject
    private PageCache pageCache;
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private NeoStores neoStores;

    @BeforeEach
    void before()
    {
        var storeFactory = new StoreFactory( databaseLayout, Config.defaults(), new DefaultIdGeneratorFactory( fs, immediate() ),
                pageCache, fs, NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @AfterEach
    void after()
    {
        neoStores.close();
    }

    @Test
    void shouldUpdateHighIdsOnExternalTransaction() throws Exception
    {
        // GIVEN
        HighIdTransactionApplier tracker = new HighIdTransactionApplier( neoStores );

        // WHEN
        // Nodes
        tracker.visitNodeCommand( Commands.createNode( 10, 2, 3 ) );
        tracker.visitNodeCommand( Commands.createNode( 20, 4, 5 ) );

        // Relationships
        tracker.visitRelationshipCommand( Commands.createRelationship( 4, 10, 20, 0 ) );
        tracker.visitRelationshipCommand( Commands.createRelationship( 45, 10, 20, 1 ) );

        // Label tokens
        tracker.visitLabelTokenCommand( Commands.createLabelToken( 3, 0 ) );
        tracker.visitLabelTokenCommand( Commands.createLabelToken( 5, 1 ) );

        // Property tokens
        tracker.visitPropertyKeyTokenCommand( Commands.createPropertyKeyToken( 3, 0 ) );
        tracker.visitPropertyKeyTokenCommand( Commands.createPropertyKeyToken( 5, 1 ) );

        // Relationship type tokens
        tracker.visitRelationshipTypeTokenCommand( Commands.createRelationshipTypeToken( 3, 0 ) );
        tracker.visitRelationshipTypeTokenCommand( Commands.createRelationshipTypeToken( 5, 1 ) );

        // Relationship groups
        tracker.visitRelationshipGroupCommand( Commands.createRelationshipGroup( 10, 1 ) );
        tracker.visitRelationshipGroupCommand( Commands.createRelationshipGroup( 20, 2 ) );

        // Schema rules
        tracker.visitSchemaRuleCommand( Commands.createIndexRule( PROVIDER_DESCRIPTOR, 10, SchemaDescriptor.forLabel( 0, 1 ) ) );
        tracker.visitSchemaRuleCommand( Commands.createIndexRule( PROVIDER_DESCRIPTOR, 20, SchemaDescriptor.forLabel( 1, 2 ) ) );

        // Properties
        tracker.visitPropertyCommand( Commands.createProperty( 10, PropertyType.STRING, 0, 6, 7 ) );
        tracker.visitPropertyCommand( Commands.createProperty( 20, PropertyType.ARRAY, 1, 8, 9 ) );

        tracker.close();

        // THEN
        assertEquals( 20 + 1, neoStores.getNodeStore().getHighId(), "NodeStore" );
        assertEquals( 5 + 1, neoStores.getNodeStore().getDynamicLabelStore().getHighId(), "DynamicNodeLabelStore" );
        assertEquals( 45 + 1, neoStores.getRelationshipStore().getHighId(), "RelationshipStore" );
        assertEquals( 5 + 1, neoStores.getRelationshipTypeTokenStore().getHighId(), "RelationshipTypeStore" );
        assertEquals( 1 + 1,
                neoStores.getRelationshipTypeTokenStore().getNameStore().getHighId(), "RelationshipType NameStore" );
        assertEquals( 5 + 1, neoStores.getPropertyKeyTokenStore().getHighId(), "PropertyKeyStore" );
        assertEquals( 1 + 1,
                neoStores.getPropertyKeyTokenStore().getNameStore().getHighId(), "PropertyKey NameStore" );
        assertEquals( 5 + 1, neoStores.getLabelTokenStore().getHighId(), "LabelStore" );
        assertEquals( 1 + 1, neoStores.getLabelTokenStore().getNameStore().getHighId(), "Label NameStore" );
        assertEquals( 20 + 1, neoStores.getPropertyStore().getHighId(), "PropertyStore" );
        assertEquals( 7 + 1, neoStores.getPropertyStore().getStringStore().getHighId(), "PropertyStore DynamicStringStore" );
        assertEquals( 9 + 1, neoStores.getPropertyStore().getArrayStore().getHighId(), "PropertyStore DynamicArrayStore" );
        assertEquals( 20 + 1, neoStores.getSchemaStore().getHighId(), "SchemaStore" );
    }

    @Test
    void shouldTrackSecondaryUnitIdsAsWell() throws Exception
    {
        // GIVEN
        HighIdTransactionApplier tracker = new HighIdTransactionApplier( neoStores );

        NodeRecord node = new NodeRecord( 5 ).initialize( true, 123, true, 456, 0 );
        node.setSecondaryUnitIdOnLoad( 6 );

        RelationshipRecord relationship = new RelationshipRecord( 10 )
                .initialize( true, 1, 2, 3, 4, 5, 6, 7, 8, true, true );
        relationship.setSecondaryUnitIdOnLoad( 12 );

        RelationshipGroupRecord relationshipGroup = new RelationshipGroupRecord( 8 )
                .initialize( true, 0, 1, 2, 3, 4, 5 );
        relationshipGroup.setSecondaryUnitIdOnLoad( 20 );

        // WHEN
        tracker.visitNodeCommand( new NodeCommand( new NodeRecord( node.getId() ), node ) );
        tracker.visitRelationshipCommand( new RelationshipCommand(
                new RelationshipRecord( relationship.getId() ), relationship ) );
        tracker.visitRelationshipGroupCommand( new RelationshipGroupCommand(
                new RelationshipGroupRecord( relationshipGroup.getId() ), relationshipGroup ) );
        tracker.close();

        // THEN
        assertEquals( node.getSecondaryUnitId() + 1, neoStores.getNodeStore().getHighId() );
        assertEquals( relationship.getSecondaryUnitId() + 1, neoStores.getRelationshipStore().getHighId() );
        assertEquals( relationshipGroup.getSecondaryUnitId() + 1, neoStores.getRelationshipGroupStore().getHighId() );
    }
}
