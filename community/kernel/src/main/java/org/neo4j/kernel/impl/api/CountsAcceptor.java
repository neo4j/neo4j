/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

public interface CountsAcceptor
{
    void incrementCountsForNode( int labelId, long delta );

    void incrementCountsForRelationship( int startLabelId, int typeId, int endLabelId, long delta );

    void incrementCountsForIndex( int labelId, int propertyKeyId, long delta );

    void replaceCountsForIndex( int labelId, int propertyKeyId, long total );

    final class Initializer implements CountsVisitor
    {
        private final CountsAcceptor target;

        public Initializer( CountsAcceptor target )
        {
            this.target = target;
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            target.incrementCountsForNode( labelId, count );
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            target.incrementCountsForRelationship( startLabelId, typeId, endLabelId, count );
        }

        @Override
        public void visitIndexCount( int labelId, int propertyKeyId, long count )
        {
            target.incrementCountsForIndex( labelId, propertyKeyId, count );
        }
    }
}
