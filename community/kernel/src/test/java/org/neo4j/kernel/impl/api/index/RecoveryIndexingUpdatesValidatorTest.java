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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class RecoveryIndexingUpdatesValidatorTest
{
    private final PrimitiveLongVisitor<RuntimeException> nodeVisitor = mock( PrimitiveLongVisitor.class );

    @Test
    public void recoveredValidatedUpdatesShouldFlushRecoveredNodeIds() throws Exception
    {
        // Given
        long nodeId1 = 42;
        long nodeId2 = 4242;
        long nodeId3 = 424242;

        IndexUpdatesValidator validator = newIndexUpdatesValidatorWithMockedDependencies();

        TransactionRepresentation tx = new PhysicalTransactionRepresentation( asList(
                nodeAddRandomLabelsCommand( nodeId1 ),
                nodeAddRandomLabelsCommand( nodeId2 ),
                nodeAddRandomLabelsCommand( nodeId3 )
        ) );

        // When
        try ( ValidatedIndexUpdates updates = validator.validate( tx ) )
        {
            updates.flush();
        }

        // Then
        verify( nodeVisitor ).visited( nodeId1 );
        verify( nodeVisitor ).visited( nodeId2 );
        verify( nodeVisitor ).visited( nodeId3 );
    }


    private IndexUpdatesValidator newIndexUpdatesValidatorWithMockedDependencies()
    {
        return new RecoveryIndexingUpdatesValidator( nodeVisitor );
    }

    private static Command nodeAddRandomLabelsCommand( long nodeId )
    {
        NodeRecord before = new NodeRecord( nodeId, true, false, NO_NEXT_RELATIONSHIP.intValue(),
                NO_NEXT_PROPERTY.intValue(), NO_LABELS_FIELD.intValue() );
        NodeRecord after = new NodeRecord( nodeId, true, false, NO_NEXT_RELATIONSHIP.intValue(),
                NO_NEXT_PROPERTY.intValue(), ThreadLocalRandom.current().nextLong( 100 ) );
        NodeCommand command = new NodeCommand();
        command.init( before, after );
        return command;
    }
}
