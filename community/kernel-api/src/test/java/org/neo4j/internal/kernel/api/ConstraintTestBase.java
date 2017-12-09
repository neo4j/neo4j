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
package org.neo4j.internal.kernel.api;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.neo4j.helpers.collection.Iterators.asList;

@SuppressWarnings( "Duplicates" )
public abstract class ConstraintTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    protected abstract LabelSchemaDescriptor labelSchemaDescriptor( int labelId, int... propertyIds );

    @Before
    public void setup()
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            for ( ConstraintDefinition definition : graphDb.schema().getConstraints() )
            {
                definition.drop();
            }
            tx.success();
        }

    }

    @Test
    public void shouldFindConstraintsBySchema() throws Exception
    {
        // GIVEN
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().constraintFor( Label.label( "FOO" ) ).assertPropertyIsUnique( "prop" ).create();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );
            int prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            LabelSchemaDescriptor descriptor = labelSchemaDescriptor( label, prop );

            //WHEN
            List<ConstraintDescriptor> constraints =
                    asList( tx.schemaRead().constraintsGetForSchema( descriptor ) );

            // THEN
            assertThat( constraints, hasSize( 1 ) );
            assertThat( constraints.get( 0 ).schema().getPropertyIds()[0], equalTo( prop) );
        }
    }

    @Test
    public void shouldFindConstraintsByLabel() throws Exception
    {
        // GIVEN
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().constraintFor( Label.label( "FOO" ) ).assertPropertyIsUnique( "prop1" ).create();
            graphDb.schema().constraintFor( Label.label( "FOO" ) ).assertPropertyIsUnique( "prop2" ).create();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().labelGetOrCreateForName( "FOO" );
            int prop1 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
            int prop2 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );

            //WHEN
            List<ConstraintDescriptor> constraints = asList(
                    tx.schemaRead().constraintsGetForLabel( label ) );

            // THEN
            assertThat( constraints, hasSize( 2 ) );
            assertThat( constraints.get( 0 ).schema().getPropertyIds(), equalTo( new int[]{prop1} ) );
            assertThat( constraints.get( 1 ).schema().getPropertyIds(), equalTo( new int[]{prop2} ) );
        }
    }
}
