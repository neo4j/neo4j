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
package org.neo4j.kernel.api.txstate;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.state.RelationshipChangesForNode;

/**
 * A visitor for visiting the changes that have been made in a transaction.
 */
public interface TxStateVisitor
{
    void visitCreatedNode( long id );

    void visitDeletedNode( long id );

    void visitCreatedRelationship( long id, int type, long startNode, long endNode );

    void visitDeletedRelationship( long id );

    void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                   Iterator<Integer> removed );

    void visitNodeRelationshipChanges( long id, RelationshipChangesForNode added,
                                       RelationshipChangesForNode removed );

    void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                  Iterator<Integer> removed );

    void visitGraphPropertyChanges( Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                    Iterator<Integer> removed );

    void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed );

    void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex );

    void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex );

    void visitAddedConstraint( UniquenessConstraint element );

    void visitRemovedConstraint( UniquenessConstraint element );

    void visitCreatedLabelToken( String name, int id );

    void visitCreatedPropertyKeyToken( String name, int id );

    void visitCreatedRelationshipTypeToken( String name, int id );

    void visitCreatedNodeLegacyIndex( String name, Map<String,String> config );

    void visitCreatedRelationshipLegacyIndex( String name, Map<String,String> config );

    class Adapter implements TxStateVisitor
    {
        @Override
        public void visitCreatedNode( long id )
        {   // Ignore
        }

        @Override
        public void visitDeletedNode( long id )
        {   // Ignore
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
        {   // Ignore
        }

        @Override
        public void visitDeletedRelationship( long id )
        {   // Ignore
        }

        @Override
        public void visitNodePropertyChanges( long id, Iterator<DefinedProperty> added,
                Iterator<DefinedProperty> changed, Iterator<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitNodeRelationshipChanges( long id, RelationshipChangesForNode added,
                RelationshipChangesForNode removed )
        {   // Ignore
        }

        @Override
        public void visitRelPropertyChanges( long id, Iterator<DefinedProperty> added,
                Iterator<DefinedProperty> changed, Iterator<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitGraphPropertyChanges( Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                Iterator<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitNodeLabelChanges( long id, Set<Integer> added, Set<Integer> removed )
        {   // Ignore
        }

        @Override
        public void visitAddedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {   // Ignore
        }

        @Override
        public void visitRemovedIndex( IndexDescriptor element, boolean isConstraintIndex )
        {   // Ignore
        }

        @Override
        public void visitAddedConstraint( UniquenessConstraint element )
        {   // Ignore
        }

        @Override
        public void visitRemovedConstraint( UniquenessConstraint element )
        {   // Ignore
        }

        @Override
        public void visitCreatedLabelToken( String name, int id )
        {   // Ignore
        }

        @Override
        public void visitCreatedPropertyKeyToken( String name, int id )
        {   // Ignore
        }

        @Override
        public void visitCreatedRelationshipTypeToken( String name, int id )
        {   // Ignore
        }

        @Override
        public void visitCreatedNodeLegacyIndex( String name, Map<String, String> config )
        {   // Ignore

        }

        @Override
        public void visitCreatedRelationshipLegacyIndex( String name, Map<String, String> config )
        {   // Ignore
        }
    }
}
