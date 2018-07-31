/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb;

import org.neo4j.graphdb.index.RelationshipIndex;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class RelationshipIndexFacadeMethods
{
    private static final FacadeMethod<RelationshipIndex> GET_WITH_FILTER = new FacadeMethod<>( "IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull, Node endNodeOrNull )", ri -> ri.get( "foo", 42, null, null ) );
    private static final FacadeMethod<RelationshipIndex> QUERY_BY_KEY_WITH_FILTER = new FacadeMethod<>( "IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )", ri -> ri.query( "foo", 42, null, null ) );
    private static final FacadeMethod<RelationshipIndex> QUERY_WITH_FILTER = new FacadeMethod<>( "IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )", ri -> ri.query( 42, null, null ) );
    private static final FacadeMethod<RelationshipIndex> GET = new FacadeMethod<>( "IndexHits<T> get( String key, Object value )", ri -> ri.get( "foo", "bar" ) );
    private static final FacadeMethod<RelationshipIndex> QUERY_BY_KEY = new FacadeMethod<>( "IndexHits<T> query( String key, Object queryOrQueryObject )", ri -> ri.query( "foo", "bar" ) );
    private static final FacadeMethod<RelationshipIndex> QUERY = new FacadeMethod<>( "IndexHits<T> query( Object queryOrQueryObject )", ri -> ri.query( "foo" ) );
    private static final FacadeMethod<RelationshipIndex> ADD = new FacadeMethod<>( "void add( T entity, String key, Object value )", ri -> ri.add( null, "foo", 42 ) );
    private static final FacadeMethod<RelationshipIndex> REMOVE_BY_KEY_AND_VALUE = new FacadeMethod<>( "void remove( T entity, String key, Object value )", ri -> ri.remove( null, "foo", 42 ) );
    private static final FacadeMethod<RelationshipIndex> REMOVE_BY_KEY = new FacadeMethod<>( "void remove( T entity, String key )", ri -> ri.remove( null, "foo" ) );
    private static final FacadeMethod<RelationshipIndex> REMOVE = new FacadeMethod<>( "void remove( T entity )", ri -> ri.remove( null ) );
    private static final FacadeMethod<RelationshipIndex> DELETE = new FacadeMethod<>( "void delete()", RelationshipIndex::delete );
    private static final FacadeMethod<RelationshipIndex> PUT_IF_ABSENT = new FacadeMethod<>( "T putIfAbsent( T entity, String key, Object value )", ri -> ri.putIfAbsent( null, "foo", 42 ) );

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

    private RelationshipIndexFacadeMethods()
    {
    }
}
