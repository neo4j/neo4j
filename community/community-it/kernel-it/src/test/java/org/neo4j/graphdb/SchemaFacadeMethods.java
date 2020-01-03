/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.function.Consumer;

import org.neo4j.graphdb.schema.Schema;

import static org.neo4j.graphdb.FacadeMethod.INDEX_DEFINITION;
import static org.neo4j.graphdb.FacadeMethod.LABEL;

public enum SchemaFacadeMethods implements Consumer<Schema>
{
    INDEX_FOR( new FacadeMethod<>( "IndexCreator indexFor( Label label )", s -> s.indexFor( LABEL ) ) ),
    GET_INDEXES_BY_LABEL( new FacadeMethod<>( "Iterable<IndexDefinition> getIndexes( Label label )", s -> s.getIndexes( LABEL ) ) ),
    GET_INDEXES( new FacadeMethod<>( "Iterable<IndexDefinition> getIndexes()", Schema::getIndexes ) ),
    GET_INDEX_STATE( new FacadeMethod<>( "IndexState getIndexState( IndexDefinition index )", s -> s.getIndexState( INDEX_DEFINITION ) ) ),
    GET_INDEX_FAILURE( new FacadeMethod<>( "String getIndexFailure( IndexDefinition index )", s -> s.getIndexFailure( INDEX_DEFINITION ) ) ),
    CONSTRAINT_FOR( new FacadeMethod<>( "ConstraintCreator constraintFor( Label label )", s -> s.constraintFor( LABEL ) ) ),
    GET_CONSTRAINTS_BY_LABEL( new FacadeMethod<>( "Iterable<ConstraintDefinition> getConstraints( Label label )", s -> s.getConstraints( LABEL ) ) ),
    GET_CONSTRAINTS( new FacadeMethod<>( "Iterable<ConstraintDefinition> getConstraints()", Schema::getConstraints ) ),
    AWAIT_INDEX_ONLINE( new FacadeMethod<>( "void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )",
            s -> s.awaitIndexOnline( INDEX_DEFINITION, 1L, TimeUnit.SECONDS ) ) ),
    AWAIT_INDEXES_ONLINE( new FacadeMethod<>( "void awaitIndexesOnline( long duration, TimeUnit unit )", s -> s.awaitIndexesOnline( 1L, TimeUnit.SECONDS ) ) );

    private final FacadeMethod<Schema> facadeMethod;

    SchemaFacadeMethods( FacadeMethod<Schema> facadeMethod )
    {
        this.facadeMethod = facadeMethod;
    }

    @Override
    public void accept( Schema schema )
    {
        facadeMethod.accept( schema );
    }

    @Override
    public String toString()
    {
        return facadeMethod.toString();
    }
}
