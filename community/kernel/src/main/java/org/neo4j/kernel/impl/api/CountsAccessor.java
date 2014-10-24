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

import org.neo4j.register.Register;

public interface CountsAccessor
{
    long nodeCount( int labelId );

    long relationshipCount( int startLabelId, int typeId, int endLabelId );

    long indexSize( int labelId, int propertyKeyId );

    long indexUpdates( int labelId, int propertyKeyId );

    void indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target );

    long incrementNodeCount( int labelId, long delta );

    long incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta );

    void replaceIndexSize( int labelId, int propertyKeyId, long total );

    long incrementIndexUpdates( int labelId, int propertyKeyId, long total );

    void replaceIndexUpdates( int labelId, int propertyKeyId, long total );

    void replaceIndexSample( int labelId, int propertyKeyId, long unique, long size );

    final class Initializer implements CountsVisitor
    {
        private final CountsAccessor target;

        public Initializer( CountsAccessor target )
        {
            this.target = target;
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            target.incrementNodeCount( labelId, count );
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            target.incrementRelationshipCount( startLabelId, typeId, endLabelId, count );
        }

        @Override
        public void visitIndexCounts( int labelId, int propertyKeyId, long updates, long size )
        {
            if ( size != -1l )
            {
                target.replaceIndexSize( labelId, propertyKeyId, size );
            }
            target.incrementIndexUpdates( labelId, propertyKeyId, updates );
        }

        @Override
        public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
        {
            target.replaceIndexSample( labelId, propertyKeyId, unique, size );
        }
    }
}
