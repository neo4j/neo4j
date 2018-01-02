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
package org.neo4j.kernel.impl.api;

public interface CountsVisitor
{
    interface Visitable
    {
        void accept( CountsVisitor visitor );
    }

    void visitNodeCount( int labelId, long count );

    void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count );

    void visitIndexStatistics( int labelId, int propertyKeyId, long updates, long size );

    void visitIndexSample( int labelId, int propertyKeyId, long unique, long size );

    public static class Adapter implements CountsVisitor
    {
        @Override
        public void visitNodeCount( int labelId, long count )
        {
            // override in subclasses
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            // override in subclasses
        }

        @Override
        public void visitIndexStatistics( int labelId, int propertyKeyId, long updates, long size )
        {
            // override in subclasses
        }

        @Override
        public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
        {
            // override in subclasses
        }

        public static CountsVisitor multiplex( final CountsVisitor... visitors )
        {
            return new CountsVisitor()
            {
                @Override
                public void visitNodeCount( int labelId, long count )
                {
                    for ( CountsVisitor visitor : visitors )
                    {
                        visitor.visitNodeCount( labelId, count );
                    }
                }

                @Override
                public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
                {
                    for ( CountsVisitor visitor : visitors )
                    {
                        visitor.visitRelationshipCount( startLabelId, typeId, endLabelId, count );
                    }
                }

                @Override
                public void visitIndexStatistics( int labelId, int propertyKeyId, long updates, long size )
                {
                    for ( CountsVisitor visitor : visitors )
                    {
                        visitor.visitIndexStatistics( labelId, propertyKeyId, updates, size );
                    }
                }

                @Override
                public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
                {
                    for ( CountsVisitor visitor : visitors )
                    {
                        visitor.visitIndexSample( labelId, propertyKeyId, unique, size );
                    }
                }
            };
        }
    }
}
