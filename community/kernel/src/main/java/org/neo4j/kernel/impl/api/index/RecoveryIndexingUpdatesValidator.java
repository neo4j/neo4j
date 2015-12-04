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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * To be used in a single-threaded scenario during recovery.
 * The design is to reduce garbage, cutting corners knowing that this is for recovery.
 * So the {@link IndexUpdatesValidator} is the same instance as the {@link ValidatedIndexUpdates} it hands out,
 * where updates are gathered for every call to {@link #validate(TransactionRepresentation)}. Now and then
 * {@link #flush(Consumer)} is be called from the recovery code, where updates (node ids) are fed into the supplied
 * {@link PrimitiveLongVisitor visitor} and a new batch can begin anew.
 */
public class RecoveryIndexingUpdatesValidator implements IndexUpdatesValidator, ValidatedIndexUpdates
{
    private final NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor();
    private final PrimitiveLongSet allNodeIds = Primitive.longSet();
    private final PrimitiveLongVisitor<RuntimeException> nodeIdCollector = new PrimitiveLongVisitor<RuntimeException>()
    {
        @Override
        public boolean visited( long value )
        {
            allNodeIds.add( value );
            return false;
        }
    };
    private final PrimitiveLongVisitor<RuntimeException> updatedNodeVisitor;

    public RecoveryIndexingUpdatesValidator( PrimitiveLongVisitor<RuntimeException> updatedNodeVisitor )
    {
        this.updatedNodeVisitor = updatedNodeVisitor;
    }

    @Override
    public ValidatedIndexUpdates validate( TransactionRepresentation transaction ) throws IOException
    {
        // Extract updates...
        try ( TransactionApplier txApplier = extractor.startTx( new TransactionToApply( transaction ) ) )
        {
            transaction.accept( txApplier );

            // and add them to the updates already existing in this batch.
            extractor.visitUpdatedNodeIds( nodeIdCollector );
            return this;
        }
        catch ( Exception e )
        {
            throw new IOException( e );
        }
    }

    @Override
    public boolean hasChanges()
    {
        return !allNodeIds.isEmpty();
    }

    @Override
    public void flush( Consumer<IndexDescriptor> affectedIndexes )
    {
        // Flush the batched changes to the supplied visitor.
        allNodeIds.visitKeys( updatedNodeVisitor );

        // And clear the updates in preparation of a new batch.
        allNodeIds.clear();
    }

    @Override
    public void close()
    {
        // Nothing in particular happens on close here, refreshing indexes or whatever will have to be done
        // externally instead.
    }
}
