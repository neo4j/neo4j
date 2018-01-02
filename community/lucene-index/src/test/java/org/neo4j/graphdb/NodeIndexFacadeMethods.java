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

import org.neo4j.graphdb.index.Index;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class NodeIndexFacadeMethods
{
    private static final FacadeMethod<Index<Node>> GET = new FacadeMethod<Index<Node>>( "IndexHits<T> get( String " +
            "key, Object value )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.get( "foo", "bar" );
        }
    };

    private static final FacadeMethod<Index<Node>> QUERY_BY_KEY = new FacadeMethod<Index<Node>>( "IndexHits<T> query(" +
            " String key, Object queryOrQueryObject )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.query( "foo", "bar" );
        }
    };

    private static final FacadeMethod<Index<Node>> QUERY = new FacadeMethod<Index<Node>>( "IndexHits<T> query( Object" +
            " queryOrQueryObject )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.query( "foo" );
        }
    };

    private static final FacadeMethod<Index<Node>> ADD = new FacadeMethod<Index<Node>>( "void add( T entity, " +
            "String key, Object value )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.add( null, "foo", 42 );
        }
    };

    private static final FacadeMethod<Index<Node>> REMOVE_BY_KEY_AND_VALUE = new FacadeMethod<Index<Node>>( "void " +
            "remove( T entity, String key, Object value )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.remove( null, "foo", 42 );
        }
    };

    private static final FacadeMethod<Index<Node>> REMOVE_BY_KEY = new FacadeMethod<Index<Node>>( "void remove( T " +
            "entity, String key )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.remove( null, "foo" );
        }
    };

    private static final FacadeMethod<Index<Node>> REMOVE = new FacadeMethod<Index<Node>>( "void remove( T entity )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.remove( null );
        }
    };

    private static final FacadeMethod<Index<Node>> DELETE = new FacadeMethod<Index<Node>>( "void delete()" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.delete();
        }
    };

    private static final FacadeMethod<Index<Node>> PUT_IF_ABSENT = new FacadeMethod<Index<Node>>( "T putIfAbsent( T " +
            "entity, String key, Object value )" )
    {
        @Override
        public void call( Index<Node> self )
        {
            self.putIfAbsent( null, "foo", 42 );
        }
    };

    static final Iterable<FacadeMethod<Index<Node>>> ALL_NODE_INDEX_FACADE_METHODS = unmodifiableCollection(
            asList(
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