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
package org.neo4j.unsafe.impl.batchimport.input;

/**
 * TODO Try to avoid String.
 */
public interface InputEntityVisitor
{
    boolean propertyId( long nextProp );

    boolean property( String key, Object value );

    // For nodes
    boolean id( long id, Group group );

    boolean id( String id, Group group );

    boolean labels( String[] labels );

    boolean labelField( long labelField );

    // For relationships
    boolean startId( long id, Group group );

    boolean startId( String id, Group group );

    boolean endId( long id, Group group );

    boolean endId( String id, Group group );

    boolean type( int type );

    boolean type( String type );

    void endOfEntity();

    class Adapter implements InputEntityVisitor
    {
        @Override
        public boolean property( String key, Object value )
        {
            return true;
        }

        @Override
        public boolean propertyId( long nextProp )
        {
            return true;
        }

        @Override
        public boolean id( long id, Group group )
        {
            return true;
        }

        @Override
        public boolean id( String id, Group group )
        {
            return true;
        }

        @Override
        public boolean labels( String[] labels )
        {
            return true;
        }

        @Override
        public boolean startId( long id, Group group )
        {
            return true;
        }

        @Override
        public boolean startId( String id, Group group )
        {
            return true;
        }

        @Override
        public boolean endId( long id, Group group )
        {
            return true;
        }

        @Override
        public boolean endId( String id, Group group )
        {
            return true;
        }

        @Override
        public boolean type( int type )
        {
            return true;
        }

        @Override
        public boolean type( String type )
        {
            return true;
        }

        @Override
        public boolean labelField( long labelField )
        {
            return true;
        }

        @Override
        public void endOfEntity()
        {
        }
    }

    class Delegate implements InputEntityVisitor
    {
        private final InputEntityVisitor actual;

        public Delegate( InputEntityVisitor actual )
        {
            this.actual = actual;
        }

        @Override
        public boolean propertyId( long nextProp )
        {
            return actual.propertyId( nextProp );
        }

        @Override
        public boolean property( String key, Object value )
        {
            return actual.property( key, value );
        }

        @Override
        public boolean id( long id, Group group )
        {
            return actual.id( id, group );
        }

        @Override
        public boolean id( String id, Group group )
        {
            return actual.id( id, group );
        }

        @Override
        public boolean labels( String[] labels )
        {
            return actual.labels( labels );
        }

        @Override
        public boolean labelField( long labelField )
        {
            return actual.labelField( labelField );
        }

        @Override
        public boolean startId( long id, Group group )
        {
            return actual.startId( id, group );
        }

        @Override
        public boolean startId( String id, Group group )
        {
            return actual.startId( id, group );
        }

        @Override
        public boolean endId( long id, Group group )
        {
            return actual.endId( id, group );
        }

        @Override
        public boolean endId( String id, Group group )
        {
            return actual.endId( id, group );
        }

        @Override
        public boolean type( int type )
        {
            return actual.type( type );
        }

        @Override
        public boolean type( String type )
        {
            return actual.type( type );
        }

        @Override
        public void endOfEntity()
        {
            actual.endOfEntity();
        }
    }
}
