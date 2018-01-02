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
package org.neo4j.graphdb;

import org.neo4j.graphdb.index.RelationshipIndex;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class RelationshipIndexFacadeMethods
{
    private static final FacadeMethod<RelationshipIndex> GET_WITH_FILTER = new FacadeMethod<RelationshipIndex>(
            "IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull, Node endNodeOrNull )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.get( "foo", 42, null, null );
        }
    };

    private static final FacadeMethod<RelationshipIndex> QUERY_BY_KEY_WITH_FILTER = new
            FacadeMethod<RelationshipIndex>( "IndexHits<Relationship> query( String key, " +
                    "Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )" )
            {
                @Override
                public void call( RelationshipIndex self )
                {
                    self.query( "foo", 42, null, null );
                }
            };

    private static final FacadeMethod<RelationshipIndex> QUERY_WITH_FILTER = new FacadeMethod<RelationshipIndex>(
            "IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull, " +
                    "Node endNodeOrNull )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.query( 42, null, null );
        }
    };

    private static final FacadeMethod<RelationshipIndex> GET = new FacadeMethod<RelationshipIndex>( "IndexHits<T>" +
            " get( String key, Object value )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.get( "foo", "bar" );
        }
    };

    private static final FacadeMethod<RelationshipIndex> QUERY_BY_KEY = new FacadeMethod<RelationshipIndex>(
            "IndexHits<T> query( String key, Object queryOrQueryObject )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.query( "foo", "bar" );
        }
    };

    private static final FacadeMethod<RelationshipIndex> QUERY = new FacadeMethod<RelationshipIndex>(
            "IndexHits<T> query( Object queryOrQueryObject )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.query( "foo" );
        }
    };

    private static final FacadeMethod<RelationshipIndex> ADD = new FacadeMethod<RelationshipIndex>( "void add( T " +
            "entity, String key, Object value )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.add( null, "foo", 42 );
        }
    };

    private static final FacadeMethod<RelationshipIndex> REMOVE_BY_KEY_AND_VALUE = new
            FacadeMethod<RelationshipIndex>( "void remove( T entity, String key, Object value )" )
            {
                @Override
                public void call( RelationshipIndex self )
                {
                    self.remove( null, "foo", 42 );
                }
            };

    private static final FacadeMethod<RelationshipIndex> REMOVE_BY_KEY = new FacadeMethod<RelationshipIndex>(
            "void remove( T entity, String key )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.remove( null, "foo" );
        }
    };

    private static final FacadeMethod<RelationshipIndex> REMOVE = new FacadeMethod<RelationshipIndex>( "void " +
            "remove( T entity )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.remove( null );
        }
    };

    private static final FacadeMethod<RelationshipIndex> DELETE = new FacadeMethod<RelationshipIndex>( "void delete()" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.delete();
        }
    };

    private static final FacadeMethod<RelationshipIndex> PUT_IF_ABSENT = new FacadeMethod<RelationshipIndex>( "T " +
            "putIfAbsent( T entity, String key, Object value )" )
    {
        @Override
        public void call( RelationshipIndex self )
        {
            self.putIfAbsent( null, "foo", 42 );
        }
    };

    static final Iterable<FacadeMethod<RelationshipIndex>> ALL_RELATIONSHIP_INDEX_FACADE_METHODS =
            unmodifiableCollection(
                    asList(
                            GET_WITH_FILTER,
                            QUERY_BY_KEY_WITH_FILTER,
                            QUERY_WITH_FILTER,
                            GET,
                            QUERY_BY_KEY,
                            QUERY,
                            ADD,
                            REMOVE_BY_KEY_AND_VALUE,
                            REMOVE_BY_KEY,
                            REMOVE,
                            DELETE,
                            PUT_IF_ABSENT
                    ) );
}