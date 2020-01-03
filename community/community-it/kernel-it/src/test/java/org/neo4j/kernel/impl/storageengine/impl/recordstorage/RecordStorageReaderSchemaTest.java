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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class RecordStorageReaderSchemaTest extends RecordStorageReaderTestBase
{
    @Test
    public void shouldListAllIndexes()
    {
        // Given
        createIndex( label1, propertyKey );
        createIndex( label2, propertyKey );

        // When
        Set<CapableIndexDescriptor> indexes = asSet( storageReader.indexesGetAll() );

        // Then
        Set<?> expectedIndexes = asSet(
                indexDescriptor( label1, propertyKey ),
                indexDescriptor( label2, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    public void shouldListAllIndexesAtTimeOfSnapshot()
    {
        // Given
        createIndex( label1, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createIndex( label2, propertyKey );
        Set<CapableIndexDescriptor> indexes = asSet( snapshot.indexesGetAll() );

        // Then
        Set<?> expectedIndexes = asSet(
                indexDescriptor( label1, propertyKey ) );

        assertEquals( expectedIndexes, indexes );
    }

    @Test
    public void gettingIndexStateOfDroppedIndexViaSnapshotShouldThrow()
    {
        // Given
        createIndex( label1, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        dropIndex( label1, propertyKey );
        try
        {
            snapshot.indexGetState( indexDescriptor( label1, propertyKey ) );
            fail( "Should have thrown exception when asking for state of dropped index." );
        }
        catch ( IndexNotFoundKernelException ignore )
        {
            // Good.
        }
    }

    @Test
    public void shouldListAllConstraints()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetAll() );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ),
                uniqueConstraintDescriptor( label2, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsAtTimeOfSnapshot()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createUniquenessConstraint( label2, propertyKey );
        Set<ConstraintDescriptor> constraints = asSet( snapshot.constraintsGetAll() );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabel()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( storageReader.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<?> expectedConstraints = asSet( uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabelAtTimeOfSnapshot()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        StorageSchemaReader snapshot = storageReader.schemaSnapshot();
        createUniquenessConstraint( label1, otherPropertyKey );
        Set<ConstraintDescriptor> constraints = asSet( snapshot.constraintsGetForLabel( labelId( label1 ) ) );

        // Then
        Set<?> expectedConstraints = asSet( uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabelAndProperty()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label1, otherPropertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet(
                storageReader.constraintsGetForSchema( uniqueConstraintDescriptor( label1, propertyKey ).schema() ) );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    private void createIndex( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propertyKey ).create();
            tx.success();
        }
    }

    private void dropIndex( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Iterable<IndexDefinition> indexes = db.schema().getIndexes( label );
            for ( IndexDefinition index : indexes )
            {
                Iterator<String> keys = index.getPropertyKeys().iterator();
                if ( keys.hasNext() && keys.next().equals( propertyKey ) && !keys.hasNext() )
                {
                    index.drop();
                }
            }
            tx.success();
        }
    }

    private void createUniquenessConstraint( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.success();
        }
    }

    private IndexDescriptor indexDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return IndexDescriptorFactory.forSchema( SchemaDescriptorFactory.forLabel( labelId, propKeyId ) );
    }

    private ConstraintDescriptor uniqueConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.uniqueForLabel( labelId, propKeyId );
    }
}
