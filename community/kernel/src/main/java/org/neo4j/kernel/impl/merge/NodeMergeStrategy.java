/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.merge;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.PrimitiveLongPredicate;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.operations.StatementTokenNameLookup;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

abstract class NodeMergeStrategy implements Comparable<NodeMergeStrategy>
{
    // order of these enum constants determines sorting order of MergeStrategies
    enum Type implements Comparable<Type>
    {
        UNIQUE_INDEX,
        REGULAR_INDEX,
        LABEL_SCAN,
        GLOBAL_SCAN
    }

    protected final Type type;
    protected final int labelId;
    protected final DefinedProperty property;

    NodeMergeStrategy( Type type, int labelId, DefinedProperty property )
    {
        this.type = type;
        this.labelId = labelId;
        this.property = property;
    }

    @Override
    public int compareTo( NodeMergeStrategy that )
    {
        // 1. sort strategies according to type
        int result = this.type.compareTo( that.type );

        if ( result == 0 )
        {
            // 2. sort by labelId
            result = Integer.compare( this.labelId, that.labelId );
            if ( result == 0 )
            {
                // 3. sort by propertyKeyId
                // this is guaranteed to not be equal by duplicate checking in
                // NodeMerge#uniqueLabelIds() and NodeMerge#withProperty()
                result = Integer.compare( this.property.propertyKeyId(), that.property.propertyKeyId() );
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode() ^ labelId ^ property.propertyKeyId();
    }

    @Override
    public boolean equals( Object obj )
    {
        return this == obj || (obj instanceof NodeMergeStrategy && compareTo( (NodeMergeStrategy) obj ) == 0);
    }

    public long merge( Statement statement, long expectedNodeId, PrimitiveLongPredicate nodePredicate )
    {
        ReadOperations readOps = statement.readOperations();
        try
        {
            long candidateId = lookupSingle( readOps, nodePredicate );

            if ( candidateId != NO_SUCH_NODE )
            {
                if ( expectedNodeId != NO_SUCH_NODE && candidateId != expectedNodeId )
                {
                    throw new NotFoundException( "Conflicting nodes found." );
                }
                expectedNodeId = candidateId;
            }
        }
        catch ( IndexBrokenKernelException e )
        {
            throw new IllegalStateException( e.getUserMessage( new StatementTokenNameLookup( readOps ) ), e );
        }
        return expectedNodeId;
    }

    protected abstract PrimitiveLongIterator lookupAll( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
            throws IndexBrokenKernelException;

    protected abstract long lookupSingle( ReadOperations ops, PrimitiveLongPredicate nodePredicate )
            throws IndexBrokenKernelException;
}
