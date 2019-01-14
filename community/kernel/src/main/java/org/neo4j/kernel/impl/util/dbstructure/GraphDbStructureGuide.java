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

import javax.xml.validation.SchemaFactory;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Visitable;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.impl.api.store.DefaultIndexReference;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;
import static org.neo4j.helpers.collection.Iterators.loop;
import static org.neo4j.internal.kernel.api.IndexReference.sortByType;
import static org.neo4j.kernel.api.StatementConstants.ANY_LABEL;
import static org.neo4j.kernel.api.StatementConstants.ANY_RELATIONSHIP_TYPE;

public class GraphDbStructureGuide implements Visitable<DbStructureVisitor>
{
    private static RelationshipType WILDCARD_REL_TYPE = () -> "";

    private final GraphDatabaseService db;
    private final ThreadToStatementContextBridge bridge;

    public GraphDbStructureGuide( GraphDatabaseService graph )
    {
        this.db = graph;
        DependencyResolver dependencies = ((GraphDatabaseAPI) graph).getDependencyResolver();
        this.bridge = dependencies.resolveDependency( ThreadToStatementContextBridge.class );
    }

    @Override
    public void accept( DbStructureVisitor visitor )
    {
        try ( Transaction tx = db.beginTx() )
        {
            showStructure( bridge.getKernelTransactionBoundToThisThread( true ), visitor );
            tx.success();
        }
    }

    private void showStructure( KernelTransaction ktx, DbStructureVisitor visitor )
    {

        try
        {
            showTokens( visitor, ktx );
            showSchema( visitor, ktx );
            showStatistics( visitor, ktx );
        }
        catch ( KernelException e )
        {
            throw new IllegalStateException( "Kernel exception when traversing database schema structure and statistics. " +
                    "This is not expected to happen.", e );
        }
    }

    private void showTokens( DbStructureVisitor visitor, KernelTransaction ktx )
    {
        showLabels( ktx, visitor );
        showPropertyKeys( ktx, visitor );
        showRelTypes( ktx, visitor );
    }

    private void showLabels( KernelTransaction ktx, DbStructureVisitor visitor )
    {
        for ( Label label : db.getAllLabels() )
        {
            int labelId = ktx.tokenRead().nodeLabel( label.name() );
            visitor.visitLabel( labelId, label.name() );
        }
    }

    private void showPropertyKeys( KernelTransaction ktx, DbStructureVisitor visitor )
    {
        for ( String propertyKeyName : db.getAllPropertyKeys() )
        {
            int propertyKeyId = ktx.tokenRead().propertyKey( propertyKeyName );
            visitor.visitPropertyKey( propertyKeyId, propertyKeyName );
        }
    }

    private void showRelTypes( KernelTransaction ktx, DbStructureVisitor visitor )
    {
        for ( RelationshipType relType : db.getAllRelationshipTypes() )
        {
            int relTypeId = ktx.tokenRead().relationshipType( relType.name() );
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
        for ( IndexReference reference : loop( sortByType( schemaRead.indexesGetAll() ) ) )
        {
            String userDescription = SchemaDescriptorFactory.forLabel( reference.label(), reference.properties() )
                    .userDescription( nameLookup );
            double uniqueValuesPercentage = schemaRead.indexUniqueValuesSelectivity( reference );
            long size = schemaRead.indexSize( reference );
            visitor.visitIndex( DefaultIndexReference.toDescriptor( reference ), userDescription, uniqueValuesPercentage, size );
        }
    }

    private void showUniqueConstraints( DbStructureVisitor visitor, KernelTransaction ktx, TokenNameLookup nameLookup )
    {
        Iterator<ConstraintDescriptor> constraints = ktx.schemaRead().constraintsGetAll();
        while ( constraints.hasNext() )
        {
            ConstraintDescriptor constraint = constraints.next();
            String userDescription = constraint.prettyPrint( nameLookup );

            if ( constraint instanceof UniquenessConstraintDescriptor )
            {
                visitor.visitUniqueConstraint( (UniquenessConstraintDescriptor) constraint, userDescription );
            }
            else if ( constraint instanceof NodeExistenceConstraintDescriptor )
            {
                NodeExistenceConstraintDescriptor existenceConstraint = (NodeExistenceConstraintDescriptor) constraint;
                visitor.visitNodePropertyExistenceConstraint( existenceConstraint, userDescription );
            }
            else if ( constraint instanceof RelExistenceConstraintDescriptor )
            {
                RelExistenceConstraintDescriptor existenceConstraint = (RelExistenceConstraintDescriptor) constraint;
                visitor.visitRelationshipPropertyExistenceConstraint( existenceConstraint, userDescription );
            }
            else if ( constraint instanceof NodeKeyConstraintDescriptor )
            {
                NodeKeyConstraintDescriptor nodeKeyConstraint = (NodeKeyConstraintDescriptor) constraint;
                visitor.visitNodeKeyConstraint( nodeKeyConstraint, userDescription );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown constraint type: " + constraint.getClass() + ", " +
                                                    "constraint: " + constraint );
            }
        }
    }

    private void showStatistics( DbStructureVisitor visitor, KernelTransaction ktx )
    {
        showNodeCounts( ktx, visitor );
        showRelCounts( ktx, visitor );
    }

    private void showNodeCounts( KernelTransaction ktx, DbStructureVisitor visitor )
    {
        Read read = ktx.dataRead();
        visitor.visitAllNodesCount( read.countsForNode( ANY_LABEL ) );
        for ( Label label : db.getAllLabels() )
        {
            int labelId = ktx.tokenRead().nodeLabel( label.name() );
            visitor.visitNodeCount( labelId, label.name(), read.countsForNode( labelId ) );
        }
    }
    private void showRelCounts( KernelTransaction ktx, DbStructureVisitor visitor )
    {
        // all wildcards
        noSide( ktx, visitor, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );

        TokenRead tokenRead = ktx.tokenRead();
        // one label only
        for ( Label label : db.getAllLabels() )
        {
            int labelId = tokenRead.nodeLabel( label.name() );

            leftSide( ktx, visitor, label, labelId, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );
            rightSide( ktx, visitor, label, labelId, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );
        }

        // fixed rel type
        for ( RelationshipType relType : db.getAllRelationshipTypes() )
        {
            int relTypeId = tokenRead.relationshipType( relType.name() );
            noSide( ktx, visitor, relType, relTypeId );

            for ( Label label : db.getAllLabels() )
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
        return  name.length() == 0 ? name : (":" + name);
    }
}
