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

import org.neo4j.graphdb.index.Index;

public enum NodeIndexFacadeMethods implements Consumer<Index<Node>>
{
    GET( new FacadeMethod<>( "IndexHits<T> get( String key, Object value )", self -> self.get( "foo", "bar" ) ) ),
    QUERY_BY_KEY( new FacadeMethod<>( "IndexHits<T> query( String key, Object queryOrQueryObject )", self -> self.query( "foo", "bar" ) ) ),
    QUERY( new FacadeMethod<>( "IndexHits<T> query( Object queryOrQueryObject )", self -> self.query( "foo" ) ) ),
    ADD( new FacadeMethod<>( "void add( T entity, String key, Object value )", self -> self.add( null, "foo", 42 ) ) ),
    REMOVE_BY_KEY_AND_VALUE( new FacadeMethod<>( "void remove( T entity, String key, Object value )", self -> self.remove( null, "foo", 42 ) ) ),
    REMOVE_BY_KEY( new FacadeMethod<>( "void remove( T entity, String key )", self -> self.remove( null, "foo" ) ) ),
    REMOVE( new FacadeMethod<>( "void remove( T entity )", self -> self.remove( null ) ) ),
    DELETE( new FacadeMethod<>( "void delete()", Index::delete ) ),
    PUT_IF_ABSENT( new FacadeMethod<>( "T putIfAbsent( T entity, String key, Object value )", self -> self.putIfAbsent( null, "foo", 42 ) ) );

    private final FacadeMethod<Index<Node>> facadeMethod;

    NodeIndexFacadeMethods( FacadeMethod<Index<Node>> facadeMethod )
    {
        this.facadeMethod = facadeMethod;
    }

    @Override
    public void accept( Index<Node> nodeIndex )
    {
        facadeMethod.accept( nodeIndex );
    }

    @Override
    public String toString()
    {
        return facadeMethod.toString();
    }
}
