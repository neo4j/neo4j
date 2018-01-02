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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class DataStatementArgumentVerificationTest
{
    @Test
    public void shouldReturnNoPropertyFromNodeGetPropertyWithoutDelegatingForNoSuchPropertyKeyIdConstant()
            throws Exception
    {
        // given
        ReadOperations statement = stubStatement();

        // when
        Value value = statement.nodeGetProperty( 17, StatementConstants.NO_SUCH_PROPERTY_KEY );

        // then
        assertTrue( "should return NoProperty", value == NO_VALUE );
    }

    @Test
    public void shouldReturnNoPropertyFromRelationshipGetPropertyWithoutDelegatingForNoSuchPropertyKeyIdConstant()
            throws Exception
    {
        // given
        ReadOperations statement = stubStatement();

        // when
        Value value = statement.relationshipGetProperty( 17, StatementConstants.NO_SUCH_PROPERTY_KEY );

        // then
        assertEquals( "should return NoProperty", value, NO_VALUE );
    }

    @Test
    public void shouldReturnNoPropertyFromGraphGetPropertyWithoutDelegatingForNoSuchPropertyKeyIdConstant()
            throws Exception
    {
        // given
        ReadOperations statement = stubStatement();

        // when
        Object value = statement.graphGetProperty( StatementConstants.NO_SUCH_PROPERTY_KEY );

        // then
        assertEquals( "should return NoProperty", value, NO_VALUE );
    }

    @Test
    public void shouldReturnEmptyIdIteratorFromNodesGetForLabelForNoSuchLabelConstant() throws Exception
    {
        // given
        ReadOperations statement = stubStatement();

        // when
        PrimitiveLongIterator nodes = statement.nodesGetForLabel( StatementConstants.NO_SUCH_LABEL );

        // then
        assertFalse( "should not contain any ids", nodes.hasNext() );
    }

    @Test
    public void shouldAlwaysReturnFalseFromNodeHasLabelForNoSuchLabelConstant() throws Exception
    {
        // given
        ReadOperations statement = stubStatement();

        // when
        boolean hasLabel = statement.nodeHasLabel( 17, StatementConstants.NO_SUCH_LABEL );

        // then
        assertFalse( "should not contain any ids", hasLabel );
    }

    private OperationsFacade stubStatement()
    {
        return new OperationsFacade( mock(KernelTransaction.class), mock( KernelStatement.class ), new Procedures(),
                mock( StatementOperationParts.class ) );
    }
}
