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

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings( "Duplicates" )
public abstract class ConstraintTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    protected abstract LabelSchemaDescriptor labelSchemaDescriptor( int labelId, int... propertyIds );

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
            Iterator<ConstraintDescriptor> constraints =
                    tx.schemaRead().constraintsGetForSchema( descriptor );

            // THEN
            ConstraintDescriptor next = constraints.next();
            assertTrue( next.enforcesUniqueness() );
            assertThat( next.schema(), equalTo( descriptor ) );
            assertFalse( constraints.hasNext() );
        }
    }
}
