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
package org.neo4j.kernel.impl.util.dbstructure;

import java.util.Iterator;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Visitable;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;

public class GraphDbStructureGuide implements Visitable<DbStructureVisitor>
{
    private static final RelationshipType WILDCARD_REL_TYPE = () -> "";

    private final GraphDatabaseAPI db;

    public GraphDbStructureGuide( GraphDatabaseService graph )
    {
        this.db = (GraphDatabaseAPI) graph;
    }

    @Override
    public void accept( DbStructureVisitor visitor )
    {
        try ( Transaction tx = db.beginTx() )
        {
            showStructure( (InternalTransaction) tx, visitor );
            tx.commit();
        }
    }

    private void showStructure( InternalTransaction transaction, DbStructureVisitor visitor )
    {

        try
        {
            showTokens( visitor, transaction );
            showSchema( visitor, transaction.kernelTransaction() );
            showStatistics( visitor, transaction );
        }
        catch ( KernelException e )
        {
            throw new IllegalStateException( "Kernel exception when traversing database schema structure and statistics. " +
                    "This is not expected to happen.", e );
        }
    }

    private void showTokens( DbStructureVisitor visitor, InternalTransaction transaction )
    {
        showLabels( transaction, visitor );
        showPropertyKeys( transaction, visitor );
        showRelTypes( transaction, visitor );
    }

    private void showLabels( InternalTransaction transaction, DbStructureVisitor visitor )
    {
        for ( Label label : transaction.getAllLabels() )
        {
            int labelId = transaction.kernelTransaction().tokenRead().nodeLabel( label.name() );
            visitor.visitLabel( labelId, label.name() );
        }
    }

    private void showPropertyKeys( InternalTransaction transaction, DbStructureVisitor visitor )
    {
        for ( String propertyKeyName : transaction.getAllPropertyKeys() )
        {
            int propertyKeyId = transaction.kernelTransaction().tokenRead().propertyKey( propertyKeyName );
            visitor.visitPropertyKey( propertyKeyId, propertyKeyName );
        }
    }

    private void showRelTypes( InternalTransaction transaction, DbStructureVisitor visitor )
    {
        for ( RelationshipType relType : transaction.getAllRelationshipTypes() )
        {
            int relTypeId = transaction.kernelTransaction().tokenRead().relationshipType( relType.name() );
            visitor.visitRelationshipType( relTypeId, relType.name() );
        }
    }

    private void showSchema( DbStructureVisitor visitor, KernelTransaction ktx ) throws IndexNotFoundKernelException
    {
        TokenNameLookup nameLookup = new SilentTokenNameLookup( ktx.tokenRead() );

        showIndices( visitor, ktx, nameLookup );
        showUniqueConstraints( visitor, ktx, nameLookup );
    }

    private void showIndices( DbStructureVisitor visitor, KernelTransaction ktx, TokenNameLookup nameLookup )
            throws IndexNotFoundKernelException
    {
        SchemaRead schemaRead = ktx.schemaRead();
        for ( IndexDescriptor reference : loop( IndexDescriptor.sortByType( schemaRead.indexesGetAll() ) ) )
        {
            String userDescription = reference.schema().userDescription( nameLookup );
            double uniqueValuesPercentage = schemaRead.indexUniqueValuesSelectivity( reference );
            long size = schemaRead.indexSize( reference );
            visitor.visitIndex( reference, userDescription, uniqueValuesPercentage, size );
        }
    }

    private void showUniqueConstraints( DbStructureVisitor visitor, KernelTransaction ktx, TokenNameLookup nameLookup )
    {
        Iterator<ConstraintDescriptor> constraints = ktx.schemaRead().constraintsGetAll();
        while ( constraints.hasNext() )
        {
            ConstraintDescriptor constraint = constraints.next();
            String userDescription = constraint.prettyPrint( nameLookup );

            if ( constraint.isUniquenessConstraint() )
            {
                visitor.visitUniqueConstraint( constraint.asUniquenessConstraint(), userDescription );
            }
            else if ( constraint.isNodePropertyExistenceConstraint() )
            {
                NodeExistenceConstraintDescriptor existenceConstraint = constraint.asNodePropertyExistenceConstraint();
                visitor.visitNodePropertyExistenceConstraint( existenceConstraint, userDescription );
            }
            else if ( constraint.isRelationshipPropertyExistenceConstraint() )
            {
                RelExistenceConstraintDescriptor existenceConstraint = constraint.asRelationshipPropertyExistenceConstraint();
                visitor.visitRelationshipPropertyExistenceConstraint( existenceConstraint, userDescription );
            }
            else if ( constraint.isNodeKeyConstraint() )
            {
                NodeKeyConstraintDescriptor nodeKeyConstraint = constraint.asNodeKeyConstraint();
                visitor.visitNodeKeyConstraint( nodeKeyConstraint, userDescription );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown constraint type: " + constraint.getClass() + ", " +
                                                    "constraint: " + constraint );
            }
        }
    }

    private void showStatistics( DbStructureVisitor visitor, InternalTransaction transaction )
    {
        showNodeCounts( transaction, visitor );
        showRelCounts( transaction, visitor );
    }

    private void showNodeCounts( InternalTransaction transaction, DbStructureVisitor visitor )
    {
        var kernelTransaction = transaction.kernelTransaction();
        Read read = kernelTransaction.dataRead();
        visitor.visitAllNodesCount( read.countsForNode( ANY_LABEL ) );
        for ( Label label : transaction.getAllLabels() )
        {
            int labelId = kernelTransaction.tokenRead().nodeLabel( label.name() );
            visitor.visitNodeCount( labelId, label.name(), read.countsForNode( labelId ) );
        }
    }
    private void showRelCounts( InternalTransaction transaction, DbStructureVisitor visitor )
    {
        // all wildcards
        KernelTransaction ktx = transaction.kernelTransaction();
        noSide( ktx, visitor, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );

        TokenRead tokenRead = ktx.tokenRead();
        // one label only
        for ( Label label : transaction.getAllLabels() )
        {
            int labelId = tokenRead.nodeLabel( label.name() );

            leftSide( ktx, visitor, label, labelId, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );
            rightSide( ktx, visitor, label, labelId, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );
        }

        // fixed rel type
        for ( RelationshipType relType : transaction.getAllRelationshipTypes() )
        {
            int relTypeId = tokenRead.relationshipType( relType.name() );
            noSide( ktx, visitor, relType, relTypeId );

            for ( Label label : transaction.getAllLabels() )
            {
                int labelId = tokenRead.nodeLabel( label.name() );

                // wildcard on right
                leftSide( ktx, visitor, label, labelId, relType, relTypeId );

                // wildcard on left
                rightSide( ktx, visitor, label, labelId, relType, relTypeId );
            }
        }
    }

    private void noSide( KernelTransaction ktx, DbStructureVisitor visitor, RelationshipType relType, int relTypeId )
    {
        String userDescription = format("MATCH ()-[%s]->() RETURN count(*)", colon( relType.name() ));
        long amount = ktx.dataRead().countsForRelationship( ANY_LABEL, relTypeId, ANY_LABEL );

        visitor.visitRelCount( ANY_LABEL, relTypeId, ANY_LABEL, userDescription, amount );
    }

    private void leftSide( KernelTransaction ktx, DbStructureVisitor visitor, Label label, int labelId,
            RelationshipType relType, int relTypeId )
    {
        String userDescription =
                format( "MATCH (%s)-[%s]->() RETURN count(*)", colon( label.name() ), colon( relType.name() ) );
        long amount = ktx.dataRead().countsForRelationship( labelId, relTypeId, ANY_LABEL );

        visitor.visitRelCount( labelId, relTypeId, ANY_LABEL, userDescription, amount );
    }

    private void rightSide( KernelTransaction ktx, DbStructureVisitor visitor, Label label, int labelId,
            RelationshipType relType, int relTypeId )
    {
        String userDescription =
                format( "MATCH ()-[%s]->(%s) RETURN count(*)", colon( relType.name() ), colon( label.name() ) );
        long amount = ktx.dataRead().countsForRelationship( ANY_LABEL, relTypeId, labelId );

        visitor.visitRelCount( ANY_LABEL, relTypeId, labelId, userDescription, amount );
    }

    private String colon( String name )
    {
        return  name.isEmpty() ? name : (":" + name);
    }
}
