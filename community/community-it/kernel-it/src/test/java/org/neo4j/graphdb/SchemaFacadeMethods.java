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

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;
import static org.mockito.Mockito.mock;

public class SchemaFacadeMethods
{
    private static final Label LABEL = Label.label( "Label" );

    private static final IndexDefinition INDEX_DEFINITION = mock( IndexDefinition.class );

    private static final FacadeMethod<Schema> INDEX_FOR = new FacadeMethod<>( "IndexCreator indexFor( Label label )", s -> s.indexFor( LABEL ) );
    private static final FacadeMethod<Schema> GET_INDEXES_BY_LABEL = new FacadeMethod<>( "Iterable<IndexDefinition> getIndexes( Label label )", s -> s.getIndexes( LABEL ) );
    private static final FacadeMethod<Schema> GET_INDEXES = new FacadeMethod<>( "Iterable<IndexDefinition> getIndexes()", Schema::getIndexes );
    private static final FacadeMethod<Schema> GET_INDEX_STATE = new FacadeMethod<>( "IndexState getIndexState( IndexDefinition index )", s -> s.getIndexState( INDEX_DEFINITION ) );
    private static final FacadeMethod<Schema> GET_INDEX_FAILURE = new FacadeMethod<>( "String getIndexFailure( IndexDefinition index )", s -> s.getIndexFailure( INDEX_DEFINITION ) );
    private static final FacadeMethod<Schema> CONSTRAINT_FOR = new FacadeMethod<>( "ConstraintCreator constraintFor( Label label )", s -> s.constraintFor( LABEL ) );
    private static final FacadeMethod<Schema> GET_CONSTRAINTS_BY_LABEL = new FacadeMethod<>( "Iterable<ConstraintDefinition> getConstraints( Label label )", s -> s.getConstraints( LABEL ) );
    private static final FacadeMethod<Schema> GET_CONSTRAINTS = new FacadeMethod<>( "Iterable<ConstraintDefinition> getConstraints()", Schema::getConstraints );
    private static final FacadeMethod<Schema> AWAIT_INDEX_ONLINE = new FacadeMethod<>( "void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )", s -> s.awaitIndexOnline( INDEX_DEFINITION, 1L, TimeUnit.SECONDS ) );
    private static final FacadeMethod<Schema> AWAIT_INDEXES_ONLINE = new FacadeMethod<>( "void awaitIndexesOnline( long duration, TimeUnit unit )", s -> s.awaitIndexesOnline( 1L, TimeUnit.SECONDS ) );

    static final Iterable<FacadeMethod<Schema>> ALL_SCHEMA_FACADE_METHODS = unmodifiableCollection( asList(
        INDEX_FOR,
        GET_INDEXES_BY_LABEL,
        GET_INDEXES,
        GET_INDEX_STATE,
        GET_INDEX_FAILURE,
        CONSTRAINT_FOR,
        GET_CONSTRAINTS_BY_LABEL,
        GET_CONSTRAINTS,
        AWAIT_INDEX_ONLINE,
        AWAIT_INDEXES_ONLINE
    ) );

    private SchemaFacadeMethods()
    {
    }
}
