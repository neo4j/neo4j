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

import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * To be used in a single-threaded scenario.
 */
public class RecoveryIndexingUpdatesValidator implements IndexUpdatesValidator, ValidatedIndexUpdates
{
    private final NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor();
    private final PrimitiveLongVisitor<RuntimeException> updatedNodeVisitor;

    public RecoveryIndexingUpdatesValidator( PrimitiveLongVisitor<RuntimeException> updatedNodeVisitor )
    {
        this.updatedNodeVisitor = updatedNodeVisitor;
    }

    @Override
    public ValidatedIndexUpdates validate( TransactionRepresentation transaction ) throws IOException
    {
        extractor.clear();
        transaction.accept( extractor );
        return extractor.containsAnyNodeOrPropertyUpdate() ? this : ValidatedIndexUpdates.NONE;
    }

    @Override
    public void flush()
    {
        extractor.visitUpdatedNodeIds( updatedNodeVisitor );
    }

    @Override
    public void close()
    {
        // Nothing in particular happens on close here, refreshing indexes or whatever will have to be done
        // externally instead.
    }

    @Override
    public boolean hasChanges()
    {
        return extractor.containsAnyNodeOrPropertyUpdate();
    }
}
