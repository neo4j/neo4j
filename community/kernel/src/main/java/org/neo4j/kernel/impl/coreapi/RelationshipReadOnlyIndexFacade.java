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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.graphdb.index.RelationshipIndex;

public class RelationshipReadOnlyIndexFacade extends
        ReadOnlyIndexFacade<Relationship> implements
        RelationshipIndex
{
    private final ReadableRelationshipIndex delegate;

    public RelationshipReadOnlyIndexFacade(
            ReadableRelationshipIndex delegate )
    {
        super( delegate );
        this.delegate = delegate;
    }

    @Override
    public IndexHits<Relationship> get( String key, Object valueOrNull,
                                        Node startNodeOrNull, Node endNodeOrNull )
    {
        return delegate.get( key,
                valueOrNull, startNodeOrNull,
                endNodeOrNull );
    }

    @Override
    public IndexHits<Relationship> query( String key,
                                          Object queryOrQueryObjectOrNull, Node startNodeOrNull,
                                          Node endNodeOrNull )
    {
        return delegate.query( key,
                queryOrQueryObjectOrNull,
                startNodeOrNull, endNodeOrNull );
    }

    @Override
    public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull,
                                          Node startNodeOrNull, Node endNodeOrNull )
    {
        return delegate.query(
                queryOrQueryObjectOrNull, startNodeOrNull,
                endNodeOrNull );
    }
}
