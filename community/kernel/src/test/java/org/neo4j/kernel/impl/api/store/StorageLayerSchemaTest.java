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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class StorageLayerSchemaTest extends StorageLayerTest
{
    @Test
    public void shouldListAllConstraints()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( disk.constraintsGetAll() );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ),
                uniqueConstraintDescriptor( label2, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    @Test
    public void shouldListAllConstraintsForLabel()
    {
        // Given
        createUniquenessConstraint( label1, propertyKey );
        createUniquenessConstraint( label2, propertyKey );

        // When
        Set<ConstraintDescriptor> constraints = asSet( disk.constraintsGetForLabel( labelId( label1 ) ) );

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
                disk.constraintsGetForSchema( uniqueConstraintDescriptor( label1, propertyKey ).schema() ) );

        // Then
        Set<?> expectedConstraints = asSet(
                uniqueConstraintDescriptor( label1, propertyKey ) );

        assertEquals( expectedConstraints, constraints );
    }

    private void createUniquenessConstraint( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            tx.success();
        }
    }

    private ConstraintDescriptor uniqueConstraintDescriptor( Label label, String propertyKey )
    {
        int labelId = labelId( label );
        int propKeyId = propertyKeyId( propertyKey );
        return ConstraintDescriptorFactory.uniqueForLabel( labelId, propKeyId );
    }
}
