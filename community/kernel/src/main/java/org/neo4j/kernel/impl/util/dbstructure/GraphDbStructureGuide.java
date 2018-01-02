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
package org.neo4j.kernel.impl.util.dbstructure;

import java.util.Iterator;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Visitable;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;
import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;
import static org.neo4j.kernel.api.ReadOperations.ANY_RELATIONSHIP_TYPE;

public class GraphDbStructureGuide implements Visitable<DbStructureVisitor>
{
    private static RelationshipType WILDCARD_REL_TYPE = new RelationshipType()
    {
        @Override
        public String name()
        {
            return "";
        }
    };

    private final GraphDatabaseAPI db;
    private final ThreadToStatementContextBridge bridge;
    private final GlobalGraphOperations glops;

    public GraphDbStructureGuide( GraphDatabaseService graph )
    {
        this.db = (GraphDatabaseAPI) graph;
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        this.bridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
        this.glops = GlobalGraphOperations.at( db );
    }

    public void accept( DbStructureVisitor visitor )
    {
        try ( Transaction tx = db.beginTx() )
        {
            try ( Statement statement = bridge.get() )
            {
                showStructure( statement, visitor );
            }
            tx.success();
        }
    }

    private void showStructure( Statement statement, DbStructureVisitor visitor )
    {
        ReadOperations read = statement.readOperations();

        try
        {
            showTokens( visitor, read );
            showSchema( visitor, read );
            showStatistics( visitor, read );
        }
        catch (KernelException e)
        {
            throw new IllegalStateException( "Kernel exception when traversing database schema structure and statistics.  This is not expected to happen.", e );
        }
    }

    private void showTokens( DbStructureVisitor visitor, ReadOperations read )
    {
        showLabels( read, visitor );
        showPropertyKeys( read, visitor );
        showRelTypes( read, visitor );
    }

    private void showLabels( ReadOperations read, DbStructureVisitor visitor )
    {
        for ( Label label : glops.getAllLabels() )
        {
            int labelId = read.labelGetForName( label.name() );
            visitor.visitLabel( labelId, label.name() );
        }
    }

    private void showPropertyKeys( ReadOperations read, DbStructureVisitor visitor )
    {
        for ( String propertyKeyName : glops.getAllPropertyKeys() )
        {
            int propertyKeyId = read.propertyKeyGetForName( propertyKeyName );
            visitor.visitPropertyKey( propertyKeyId, propertyKeyName );
        }
    }

    private void showRelTypes( ReadOperations read, DbStructureVisitor visitor )
    {
        for ( RelationshipType relType : glops.getAllRelationshipTypes() )
        {
            int relTypeId = read.relationshipTypeGetForName( relType.name() );
            visitor.visitRelationshipType( relTypeId, relType.name() );
        }
    }

    private void showSchema( DbStructureVisitor visitor, ReadOperations read ) throws IndexNotFoundKernelException
    {
        TokenNameLookup nameLookup = new StatementTokenNameLookup( read );

        showIndices( visitor, read, nameLookup );
        showUniqueIndices( visitor, read, nameLookup );
        showUniqueConstraints( visitor, read, nameLookup );
    }

    private void showIndices( DbStructureVisitor visitor, ReadOperations read, TokenNameLookup nameLookup ) throws IndexNotFoundKernelException
    {
        Iterator<IndexDescriptor> indexDescriptors = read.indexesGetAll();
        while ( indexDescriptors.hasNext() )
        {
            IndexDescriptor descriptor = indexDescriptors.next();
            String userDescription = descriptor.userDescription( nameLookup );
            double uniqueValuesPercentage = read.indexUniqueValuesSelectivity( descriptor );
            long size = read.indexSize( descriptor );
            visitor.visitIndex( descriptor, userDescription , uniqueValuesPercentage, size );
        }
    }

    private void showUniqueIndices( DbStructureVisitor visitor, ReadOperations read, TokenNameLookup nameLookup ) throws IndexNotFoundKernelException

    {
        Iterator<IndexDescriptor> indexDescriptors = read.uniqueIndexesGetAll();
        while ( indexDescriptors.hasNext() )
        {
            IndexDescriptor descriptor = indexDescriptors.next();
            String userDescription = descriptor.userDescription( nameLookup );
            double uniqueValuesPercentage = read.indexUniqueValuesSelectivity( descriptor );
            long size = read.indexSize( descriptor );
            visitor.visitUniqueIndex( descriptor, userDescription, uniqueValuesPercentage, size );
        }
    }

    private void showUniqueConstraints( DbStructureVisitor visitor, ReadOperations read, TokenNameLookup nameLookup )
    {
        Iterator<PropertyConstraint> constraints = read.constraintsGetAll();
        while ( constraints.hasNext() )
        {
            PropertyConstraint constraint = constraints.next();
            String userDescription = constraint.userDescription( nameLookup );

            if ( constraint instanceof UniquenessConstraint )
            {
                visitor.visitUniqueConstraint( (UniquenessConstraint) constraint, userDescription );
            }
            else if ( constraint instanceof NodePropertyExistenceConstraint )
            {
                NodePropertyExistenceConstraint existenceConstraint = (NodePropertyExistenceConstraint) constraint;
                visitor.visitNodePropertyExistenceConstraint( existenceConstraint, userDescription );
            }
            else if ( constraint instanceof RelationshipPropertyExistenceConstraint )
            {
                RelationshipPropertyExistenceConstraint existenceConstraint = (RelationshipPropertyExistenceConstraint) constraint;
                visitor.visitRelationshipPropertyExistenceConstraint( existenceConstraint, userDescription );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown constraint type: " + constraint.getClass() + ", " +
                                                    "constraint: " + constraint );
            }
        }
    }

    private void showStatistics( DbStructureVisitor visitor, ReadOperations read )
    {
        showNodeCounts( read, visitor );
        showRelCounts( read, visitor );
    }

    private void showNodeCounts( ReadOperations read, DbStructureVisitor visitor )
    {
        visitor.visitAllNodesCount( read.countsForNode( ANY_LABEL ) );
        for ( Label label : glops.getAllLabels() )
        {
            int labelId = read.labelGetForName( label.name() );
            visitor.visitNodeCount( labelId, label.name(), read.countsForNode( labelId ) );
        }
    }
    private void showRelCounts( ReadOperations read, DbStructureVisitor visitor )
    {
        // all wildcards
        noSide( read, visitor, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );

        // one label only
        for ( Label label : glops.getAllLabels() )
        {
            int labelId = read.labelGetForName( label.name() );

            leftSide( read, visitor, label, labelId, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );
            rightSide( read, visitor, label, labelId, WILDCARD_REL_TYPE, ANY_RELATIONSHIP_TYPE );
        }

        // fixed rel type
        for ( RelationshipType relType : glops.getAllRelationshipTypes() )
        {
            int relTypeId = read.relationshipTypeGetForName( relType.name() );
            noSide( read, visitor, relType, relTypeId );

            for ( Label label : glops.getAllLabels() )
            {
                int labelId = read.labelGetForName( label.name() );

                // wildcard on right
                leftSide( read, visitor, label, labelId, relType, relTypeId );

                // wildcard on left
                rightSide( read, visitor, label, labelId, relType, relTypeId );
            }
        }
    }

    private void noSide( ReadOperations read, DbStructureVisitor visitor, RelationshipType relType, int relTypeId )
    {
        String userDescription = format("MATCH ()-[%s]->() RETURN count(*)", colon( relType.name() ));
        long amount = read.countsForRelationship( ANY_LABEL, relTypeId, ANY_LABEL );

        visitor.visitRelCount( ANY_LABEL, relTypeId, ANY_LABEL, userDescription, amount );
    }

    private void leftSide( ReadOperations read, DbStructureVisitor visitor, Label label, int labelId, RelationshipType relType, int relTypeId )
    {
        String userDescription = format( "MATCH (%s)-[%s]->() RETURN count(*)", colon( label.name() ), colon( relType.name() ) );
        long amount = read.countsForRelationship( labelId, relTypeId, ANY_LABEL );

        visitor.visitRelCount( labelId, relTypeId, ANY_LABEL, userDescription, amount );
    }

    private void rightSide( ReadOperations read, DbStructureVisitor visitor, Label label, int labelId, RelationshipType relType, int relTypeId )
    {
        String userDescription = format( "MATCH ()-[%s]->(%s) RETURN count(*)", colon( relType.name() ), colon( label.name() ) );
        long amount = read.countsForRelationship( ANY_LABEL, relTypeId, labelId );

        visitor.visitRelCount( ANY_LABEL, relTypeId, labelId, userDescription, amount );
    }

    private String colon( String name )
    {
        return  name.length() == 0 ? name : (":" + name);
    }
}
