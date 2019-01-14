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

import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.graphdb.schema.IndexDefinition;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

public class FacadeMethod<T> implements Consumer<T>
{
    public static final Label LABEL = Label.label( "Label" );
    public static final RelationshipType FOO = withName( "foo" );
    public static final RelationshipType BAR = withName( "bar" );
    public static final Label QUUX = label( "quux" );
    public static final IndexDefinition INDEX_DEFINITION = mock( IndexDefinition.class );

    private final String methodSignature;
    private final Consumer<T> callable;

    public FacadeMethod( String methodSignature, Consumer<T> callable )
    {
        this.methodSignature = methodSignature;
        this.callable = callable;
    }

    @Override
    public void accept( T t )
    {
        callable.accept( t );
    }

    public void call( T self )
    {
        callable.accept( self );
    }

    @Override
    public String toString()
    {
        return methodSignature;
    }

    public static <T> void consume( Iterator<T> iterator )
    {
        Iterable<T> iterable = () -> iterator;
        consume( iterable );
    }

    public static void consume( Iterable<?> iterable )
    {
        for ( Object o : iterable )
        {
            assertNotNull( o );
        }
    }
}
