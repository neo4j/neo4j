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

import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

public class RelationshipExplicitIndexProxy extends ExplicitIndexProxy<Relationship> implements RelationshipIndex
{
    public RelationshipExplicitIndexProxy( String name, GraphDatabaseService gds,
                                         Supplier<KernelTransaction> txBridge )
    {
        super( name, RELATIONSHIP, gds, txBridge );
    }

    @Override
    public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull, Node endNodeOrNull )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = ktx.cursors().allocateRelationshipExplicitIndexCursor();
            long source = startNodeOrNull == null ? NO_SUCH_NODE : startNodeOrNull.getId();
            long target = endNodeOrNull == null ? NO_SUCH_NODE : endNodeOrNull.getId();
            ktx.indexRead().relationshipExplicitIndexLookup( cursor, name, key, valueOrNull, source, target );
            return new CursorWrappingRelationshipIndexHits( cursor, getGraphDatabase(), ktx, name );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull, Node startNodeOrNull,
            Node endNodeOrNull )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = ktx.cursors().allocateRelationshipExplicitIndexCursor();
            long source = startNodeOrNull == null ? NO_SUCH_NODE : startNodeOrNull.getId();
            long target = endNodeOrNull == null ? NO_SUCH_NODE : endNodeOrNull.getId();
            ktx.indexRead().relationshipExplicitIndexQuery( cursor, name, key, queryOrQueryObjectOrNull, source, target );
            return new CursorWrappingRelationshipIndexHits( cursor, getGraphDatabase(), ktx, name );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )
    {
        KernelTransaction ktx = txBridge.get();
        try ( Statement ignore = ktx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = ktx.cursors().allocateRelationshipExplicitIndexCursor();
            long source = startNodeOrNull == null ? NO_SUCH_NODE : startNodeOrNull.getId();
            long target = endNodeOrNull == null ? NO_SUCH_NODE : endNodeOrNull.getId();
            ktx.indexRead().relationshipExplicitIndexQuery( cursor, name, queryOrQueryObjectOrNull, source, target );
            return new CursorWrappingRelationshipIndexHits( cursor, getGraphDatabase(), ktx, name );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }
}
