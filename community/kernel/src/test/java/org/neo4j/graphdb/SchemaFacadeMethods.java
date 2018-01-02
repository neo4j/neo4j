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

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

import static org.mockito.Mockito.mock;

public class SchemaFacadeMethods
{
    private static final Label LABEL = DynamicLabel.label( "Label" );

    private static final IndexDefinition INDEX_DEFINITION = mock( IndexDefinition.class );


    private static final FacadeMethod<Schema> INDEX_FOR = new FacadeMethod<Schema>( "IndexCreator indexFor( Label label )" )
    {
        @Override
        public void call( Schema self )
        {
            self.indexFor( LABEL );
        }
    };

    private static final FacadeMethod<Schema> GET_INDEXES_BY_LABEL =
            new FacadeMethod<Schema>( "Iterable<IndexDefinition> getIndexes( Label label )" )
    {
                @Override
                public void call( Schema self )
                {
                    self.getIndexes( LABEL );
                }
            };

    private static final FacadeMethod<Schema> GET_INDEXES =
        new FacadeMethod<Schema>( "Iterable<IndexDefinition> getIndexes()" )
    {
            @Override
            public void call( Schema self )
            {
                self.getIndexes();
            }
        };

    private static final FacadeMethod<Schema> GET_INDEX_STATE =
        new FacadeMethod<Schema>( "IndexState getIndexState( IndexDefinition index )" )
    {
            @Override
            public void call( Schema self )
            {
                self.getIndexState( INDEX_DEFINITION );
            }
        };

    private static final FacadeMethod<Schema> GET_INDEX_FAILURE =
        new FacadeMethod<Schema>( "String getIndexFailure( IndexDefinition index )" )
    {
            @Override
            public void call( Schema self )
            {
                self.getIndexFailure( INDEX_DEFINITION );
            }
        };

    private static final FacadeMethod<Schema> CONSTRAINT_FOR =
        new FacadeMethod<Schema>( "ConstraintCreator constraintFor( Label label )" )
    {
            @Override
            public void call( Schema self )
            {
                self.constraintFor( LABEL );
            }
        };

    private static final FacadeMethod<Schema> GET_CONSTRAINTS_BY_LABEL =
        new FacadeMethod<Schema>( "Iterable<ConstraintDefinition> getConstraints( Label label )" )
    {
            @Override
            public void call( Schema self )
            {
                self.getConstraints( LABEL );
            }
        };

    private static final FacadeMethod<Schema> GET_CONSTRAINTS =
        new FacadeMethod<Schema>( "Iterable<ConstraintDefinition> getConstraints()" )
    {
            @Override
            public void call( Schema self )
            {
                self.getConstraints();
            }
        };

    private static final FacadeMethod<Schema> AWAIT_INDEX_ONLINE =
        new FacadeMethod<Schema>( "void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )" )
    {
            @Override
            public void call( Schema self )
            {
                self.awaitIndexOnline( INDEX_DEFINITION, 1l, TimeUnit.SECONDS );
            }
        };

    private static final FacadeMethod<Schema> AWAIT_INDEXES_ONLINE =
        new FacadeMethod<Schema>( "void awaitIndexesOnline( long duration, TimeUnit unit )" )
    {
            @Override
            public void call( Schema self )
            {
                self.awaitIndexesOnline( 1l, TimeUnit.SECONDS );
            }
        };

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
}
