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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;

public class RelationshipExplicitIndexProxy extends ExplicitIndexProxy<Relationship> implements RelationshipIndex
{
    public RelationshipExplicitIndexProxy( String name, GraphDatabaseService gds,
                                         Supplier<Statement> statementContextBridge )
    {
        super( name, Type.RELATIONSHIP, gds, statementContextBridge );
    }

    @Override
    public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull, Node endNodeOrNull )
    {
        try ( Statement statement = statementContextBridge.get() )
        {
            return wrapIndexHits( statement.readOperations().relationshipExplicitIndexGet( name, key, valueOrNull,
                    entityId( startNodeOrNull ), entityId( endNodeOrNull ) ) );
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
        try ( Statement statement = statementContextBridge.get() )
        {
            return wrapIndexHits( statement.readOperations().relationshipExplicitIndexQuery( name, key,
                    queryOrQueryObjectOrNull, entityId( startNodeOrNull ), entityId( endNodeOrNull ) ) );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    @Override
    public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )
    {
        try ( Statement statement = statementContextBridge.get() )
        {
            return wrapIndexHits( statement.readOperations().relationshipExplicitIndexQuery( name,
                    queryOrQueryObjectOrNull, entityId( startNodeOrNull ), entityId( endNodeOrNull ) ) );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new NotFoundException( type + " index '" + name + "' doesn't exist" );
        }
    }

    private long entityId( Node nodeOrNull )
    {
        return nodeOrNull == null ? -1L : nodeOrNull.getId();
    }
}
