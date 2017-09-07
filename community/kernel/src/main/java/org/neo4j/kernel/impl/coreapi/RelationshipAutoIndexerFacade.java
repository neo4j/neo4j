/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi;

import java.util.function.Supplier;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;

public class RelationshipAutoIndexerFacade extends AutoIndexerFacade<Relationship> implements RelationshipAutoIndexer
{
    private final Supplier<ReadableRelationshipIndex> idxSupplier;

    public RelationshipAutoIndexerFacade( Supplier<ReadableRelationshipIndex> idxSupplier,
                                          AutoIndexOperations autoIndexing )
    {
        super( idxSupplier::get, autoIndexing );
        this.idxSupplier = idxSupplier;
    }

    @Override
    public ReadableRelationshipIndex getAutoIndex()
    {
        return idxSupplier.get();
    }
}
