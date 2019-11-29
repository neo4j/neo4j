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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;

class PropertyIT extends KernelIntegrationTest
{
    @Test
    void shouldListAllPropertyKeys() throws Exception
    {
        // given
        dbWithNoCache();

        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );

        // when
        Iterator<NamedToken> propIdsBeforeCommit = transaction.tokenRead().propertyKeyGetAllTokens();

        // then
        assertThat( asCollection( propIdsBeforeCommit ) ).contains( new NamedToken( "prop1", prop1 ), new NamedToken( "prop2", prop2 ) );

        // when
        commit();
        transaction = newTransaction();
        Iterator<NamedToken> propIdsAfterCommit = transaction.tokenRead().propertyKeyGetAllTokens();

        // then
        assertThat( asCollection( propIdsAfterCommit ) ).contains( new NamedToken( "prop1", prop1 ), new NamedToken( "prop2", prop2 ) );
        commit();
    }

    @Test
    void shouldNotAllowModifyingPropertiesOnDeletedRelationship() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int type = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "RELATED" );
        long startNodeId = transaction.dataWrite().nodeCreate();
        long endNodeId = transaction.dataWrite().nodeCreate();
        long rel = transaction.dataWrite().relationshipCreate( startNodeId, type, endNodeId );

        transaction.dataWrite().relationshipSetProperty( rel, prop1, Values.stringValue( "As" ) );
        transaction.dataWrite().relationshipDelete( rel );

        // When
        var e = assertThrows( EntityNotFoundException.class, () -> transaction.dataWrite().relationshipRemoveProperty( rel, prop1 ) );
        assertThat( e.getMessage() ).isEqualTo( "Unable to load RELATIONSHIP with id " + rel + "." );
        commit();
    }

    @Test
    void shouldBeAbleToRemoveResetAndTwiceRemovePropertyOnRelationship() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        int type = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "RELATED" );

        long startNodeId = transaction.dataWrite().nodeCreate();
        long endNodeId = transaction.dataWrite().nodeCreate();
        long rel = transaction.dataWrite().relationshipCreate( startNodeId, type, endNodeId );
        transaction.dataWrite().relationshipSetProperty( rel, prop, Values.of( "bar" ) );

        commit();

        // when
        Write write = dataWriteInNewTransaction();
        write.relationshipRemoveProperty( rel, prop );
        write.relationshipSetProperty( rel, prop, Values.of( "bar" ) );
        write.relationshipRemoveProperty( rel, prop );
        write.relationshipRemoveProperty( rel, prop );

        commit();

        // then
        transaction = newTransaction();
        assertThat( relationshipGetProperty( transaction, rel, prop ) ).isEqualTo( Values.NO_VALUE );
        commit();
    }
}

