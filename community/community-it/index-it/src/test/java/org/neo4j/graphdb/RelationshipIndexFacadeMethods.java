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
package org.neo4j.graphdb;

import java.util.function.Consumer;

import org.neo4j.graphdb.index.RelationshipIndex;

public enum RelationshipIndexFacadeMethods implements Consumer<RelationshipIndex>
{
    GET_WITH_FILTER( new FacadeMethod<>( "IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull, Node endNodeOrNull )",
            ri -> ri.get( "foo", 42, null, null ) ) ),
    QUERY_BY_KEY_WITH_FILTER(
            new FacadeMethod<>( "IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )",
                    ri -> ri.query( "foo", 42, null, null ) ) ),
    QUERY_WITH_FILTER( new FacadeMethod<>( "IndexHits<Relationship> query( Object queryOrQueryObjectOrNull, Node startNodeOrNull, Node endNodeOrNull )",
            ri -> ri.query( 42, null, null ) ) ),
    GET( new FacadeMethod<>( "IndexHits<T> get( String key, Object value )", ri -> ri.get( "foo", "bar" ) ) ),
    QUERY_BY_KEY( new FacadeMethod<>( "IndexHits<T> query( String key, Object queryOrQueryObject )", ri -> ri.query( "foo", "bar" ) ) ),
    QUERY( new FacadeMethod<>( "IndexHits<T> query( Object queryOrQueryObject )", ri -> ri.query( "foo" ) ) ),
    ADD( new FacadeMethod<>( "void add( T entity, String key, Object value )", ri -> ri.add( null, "foo", 42 ) ) ),
    REMOVE_BY_KEY_AND_VALUE( new FacadeMethod<>( "void remove( T entity, String key, Object value )", ri -> ri.remove( null, "foo", 42 ) ) ),
    REMOVE_BY_KEY( new FacadeMethod<>( "void remove( T entity, String key )", ri -> ri.remove( null, "foo" ) ) ),
    REMOVE( new FacadeMethod<>( "void remove( T entity )", ri -> ri.remove( null ) ) ),
    DELETE( new FacadeMethod<>( "void delete()", RelationshipIndex::delete ) ),
    PUT_IF_ABSENT( new FacadeMethod<>( "T putIfAbsent( T entity, String key, Object value )", ri -> ri.putIfAbsent( null, "foo", 42 ) ) );

    private final FacadeMethod<RelationshipIndex> facadeMethod;

    RelationshipIndexFacadeMethods( FacadeMethod<RelationshipIndex> facadeMethod )
    {
        this.facadeMethod = facadeMethod;
    }

    @Override
    public void accept( RelationshipIndex relationshipIndex )
    {
        facadeMethod.accept( relationshipIndex );
    }

    @Override
    public String toString()
    {
        return facadeMethod.toString();
    }
}
