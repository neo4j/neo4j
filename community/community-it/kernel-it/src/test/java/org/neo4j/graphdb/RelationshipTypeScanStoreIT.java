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
package org.neo4j.graphdb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.index.label.TokenScanReader;
import org.neo4j.internal.index.label.TokenScanStore;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.FeatureToggles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
class RelationshipTypeScanStoreIT
{
    private static final RelationshipType REL_TYPE = RelationshipType.withName( "REL_TYPE" );
    @Inject
    DatabaseManagementService dbms;
    @Inject
    GraphDatabaseService db;
    @Inject
    DbmsController dbmsController;
    @Inject
    FileSystemAbstraction fs;

    @BeforeAll
    static void toggleOn()
    {
        FeatureToggles.set( TokenScanStore.class, TokenScanStore.RELATIONSHIP_TYPE_SCAN_STORE_ENABLE_STRING, true );
    }

    @AfterAll
    static void toggleOff()
    {
        FeatureToggles.clear( TokenScanStore.class, TokenScanStore.RELATIONSHIP_TYPE_SCAN_STORE_ENABLE_STRING );
    }

    @Test
    void shouldBePossibleToResolveDependency()
    {
        getRelationshipTypeScanStore();
        // Should not throw
    }

    @Test
    void shouldSeeAddedRelationship()
    {
        List<Long> expectedIds = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = createRelationship( tx );
            expectedIds.add( relationship.getId() );
            tx.commit();
        }

        assertContainIds( expectedIds );
    }

    @Test
    void shouldNotSeeRemovedRelationship()
    {
        List<Long> expectedIds = new ArrayList<>();
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( TestLabels.LABEL_ONE );
            nodeId = node.getId();
            Relationship relationship1 = node.createRelationshipTo( tx.createNode(), REL_TYPE );
            Relationship relationship2 = createRelationship( tx );
            expectedIds.add( relationship1.getId() );
            expectedIds.add( relationship2.getId() );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
                expectedIds.remove( relationship.getId() );
            }
            tx.commit();
        }

        assertContainIds( expectedIds );
    }

    @Test
    void shouldRebuildIfMissingDuringStartup()
    {
        List<Long> expectedIds = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            Relationship relationship = createRelationship( tx );
            expectedIds.add( relationship.getId() );
            tx.commit();
        }

        ResourceIterator<File> files = getRelationshipTypeScanStoreFiles();
        dbmsController.restartDbms( builder ->
        {
            files.forEachRemaining( file -> fs.deleteFile( file ) );
            return builder;
        });

        assertContainIds( expectedIds );
    }

    private ResourceIterator<File> getRelationshipTypeScanStoreFiles()
    {
        RelationshipTypeScanStore relationshipTypeScanStore = getRelationshipTypeScanStore();
        return relationshipTypeScanStore.snapshotStoreFiles();
    }

    private Relationship createRelationship( Transaction tx )
    {
        return tx.createNode().createRelationshipTo( tx.createNode(), REL_TYPE );
    }

    private void assertContainIds( List<Long> expectedIds )
    {
        int relationshipTypeId = getRelationshipTypeId();
        RelationshipTypeScanStore relationshipTypeScanStore = getRelationshipTypeScanStore();
        TokenScanReader tokenScanReader = relationshipTypeScanStore.newReader();
        PrimitiveLongResourceIterator relationships = tokenScanReader.entityWithToken( relationshipTypeId, NULL );
        List<Long> additionalRelationships = new ArrayList<>();
        while ( relationships.hasNext() )
        {
            long next = relationships.next();
            if ( !expectedIds.remove( next ) )
            {
                additionalRelationships.add( next );
            }
        }
        assertThat( expectedIds ).as( "expected ids" ).isEmpty();
        assertThat( additionalRelationships ).as( "additional ids" ).isEmpty();
    }

    private int getRelationshipTypeId()
    {
        int relationshipTypeId;
        try ( Transaction tx = db.beginTx() )
        {
            relationshipTypeId = ((InternalTransaction) tx).kernelTransaction().tokenRead().relationshipType( REL_TYPE.name() );
            tx.commit();
        }
        return relationshipTypeId;
    }

    private RelationshipTypeScanStore getRelationshipTypeScanStore()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( RelationshipTypeScanStore.class );
    }
}
