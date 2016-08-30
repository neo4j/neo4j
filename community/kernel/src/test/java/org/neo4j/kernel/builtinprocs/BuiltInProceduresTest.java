/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.storageengine.api.Token;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.proc.CallableProcedure.Context.KERNEL_TRANSACTION;

public class BuiltInProceduresTest
{
    private final List<IndexDescriptor> indexes = new LinkedList<>();
    private final List<IndexDescriptor> uniqueIndexes = new LinkedList<>();
    private final List<PropertyConstraint> constraints = new LinkedList<>();
    private final Map<Integer, String> labels = new HashMap<>();
    private final Map<Integer, String> propKeys = new HashMap<>();
    private final Map<Integer, String> relTypes = new HashMap<>();

    private final ReadOperations read = mock(ReadOperations.class);
    private final Statement statement = mock(Statement.class);
    private final KernelTransaction tx = mock(KernelTransaction.class);

    private final Procedures procs = new Procedures();

    @Test
    public void shouldListAllIndexes() throws Throwable
    {
        // Given
        givenIndex( "User", "name" );

        // When/Then
        assertThat( call("db.indexes"),
            contains( record( "INDEX ON :User(name)", "ONLINE", "node_label_property" ) ) );
    }

    @Test
    public void shouldListAllUniqueIndexes() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );

        // When/Then
        assertThat( call( "db.indexes" ),
                contains( record( "INDEX ON :User(name)", "ONLINE", "node_unique_property" ) ) );
    }

    @Test
    public void shouldListPropertyKeys() throws Throwable
    {
        // Given
        givenPropertyKeys( "name", "age" );

        // When/Then
        assertThat( call("db.propertyKeys"),
                containsInAnyOrder(
                    record( "age" ),
                    record( "name" ) ) );
    }

    @Test
    public void shouldListLabels() throws Throwable
    {
        // Given
        givenLabels( "Banana", "Fruit" );

        // When/Then
        assertThat( call("db.labels"),
                containsInAnyOrder(
                        record( "Banana" ),
                        record( "Fruit" ) ) );
    }

    @Test
    public void shouldListRelTypes() throws Throwable
    {
        // Given
        givenRelationshipTypes( "EATS", "SPROUTS" );

        // When/Then
        assertThat( call("db.relationshipTypes"),
                containsInAnyOrder(
                        record( "EATS" ),
                        record( "SPROUTS" ) ) );
    }

    @Test
    public void shouldListConstraints() throws Throwable
    {
        // Given
        givenUniqueConstraint( "User", "name" );
        givenNodePropExistenceConstraint( "User", "name" );

        // When/Then
        assertThat( call("db.constraints"),
                contains(
                    record( "CONSTRAINT ON ( user:User ) ASSERT exists(user.name)" ),
                    record( "CONSTRAINT ON ( user:User ) ASSERT user.name IS UNIQUE" ) ) );
    }

    @Test
    public void shouldEscapeLabelNameContainingColons() throws Throwable
    {
        // Given
        givenUniqueConstraint( "FOO:BAR", "x.y" );
        givenNodePropExistenceConstraint( "FOO:BAR", "x.y" );

        // When/Then
        List<Object[]> call = call( "db.constraints" );
        assertThat( call,
                contains(
                        record( "CONSTRAINT ON ( `foo:bar`:`FOO:BAR` ) ASSERT `foo:bar`.x.y IS UNIQUE" ),
                        record( "CONSTRAINT ON ( `foo:bar`:`FOO:BAR` ) ASSERT exists(`foo:bar`.x.y)" ) ) );
    }

    @Test
    public void shouldListCorrectBuiltinProcedures() throws Throwable
    {
        // When/Then
        assertThat( call( "dbms.procedures" ), contains(
            record( "db.awaitIndex", "db.awaitIndex(label :: STRING?, property :: STRING?, timeOutSeconds :: INTEGER?) :: VOID", "Await indexes in the database to come online." ),
            record( "db.constraints", "db.constraints() :: (description :: STRING?)", "List all constraints in the database." ),
            record( "db.indexes", "db.indexes() :: (description :: STRING?, state :: STRING?, type :: STRING?)", "List all indexes in the database." ),
            record( "db.labels", "db.labels() :: (label :: STRING?)", "List all labels in the database." ),
            record( "db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING?)", "List all property keys in the database." ),
            record( "db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING?)", "List all relationship types in the database." ),
            record( "dbms.components", "dbms.components() :: (name :: STRING?, versions :: LIST? OF STRING?, edition :: STRING?)", "List DBMS components and their versions." ),
            record( "dbms.procedures", "dbms.procedures() :: (name :: STRING?, signature :: STRING?, description :: STRING?)", "List all procedures in the DBMS." ),
            record( "dbms.queryJmx", "dbms.queryJmx(query :: STRING?) :: (name :: STRING?, description :: STRING?, attributes :: MAP?)", "Query JMX management data by domain and name. For instance, \"org.neo4j:*\"")
        ) );
    }

    @Test
    public void shouldListSystemComponents() throws Throwable
    {
        // When/Then
        assertThat( call( "dbms.components" ), contains(
            record( "Neo4j Kernel", singletonList( "1.3.37" ), "enterprise" )
        ) );
    }

    private Matcher<Object[]> record( Object ... fields )
    {
        return equalTo( fields );
    }

    private void givenIndex( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        indexes.add( new IndexDescriptor( labelId, propId ) );
    }

    private void givenUniqueConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        uniqueIndexes.add( new IndexDescriptor( labelId, propId )  );
        constraints.add( new UniquenessConstraint( labelId, propId ) );
    }

    private void givenNodePropExistenceConstraint( String label, String propKey )
    {
        int labelId = token( label, labels );
        int propId = token( propKey, propKeys );

        constraints.add( new NodePropertyExistenceConstraint( labelId, propId ) );
    }

    private void givenPropertyKeys( String ... keys )
    {
        for ( String key : keys )
        {
            token(key, propKeys);
        }
    }

    private void givenLabels( String ... labelNames )
    {
        for ( String key : labelNames )
        {
            token(key, labels);
        }
    }

    private void givenRelationshipTypes( String ... types )
    {
        for ( String key : types )
        {
            token(key, relTypes);
        }
    }

    private Integer token( String name, Map<Integer,String> tokens )
    {
        return tokens.entrySet().stream()
                .filter( ( entry ) -> entry.getValue().equals( name ) )
                .map( Map.Entry::getKey )
                .findFirst().orElseGet( () -> {
                    int newIndex = tokens.size();
                    tokens.put( newIndex, name );
                    return newIndex;
                });
    }

    @Before
    public void setup() throws Exception
    {
        procs.registerComponent( KernelTransaction.class, ( ctx ) -> ctx.get( KERNEL_TRANSACTION ) );
        new SpecialBuiltInProcedures("1.3.37", Edition.enterprise.toString() ).accept( procs );
        new BuiltInProceduresProvider().registerProcedures( procs );

        when(tx.acquireStatement()).thenReturn( statement );
        when(statement.readOperations()).thenReturn( read );

        when(read.propertyKeyGetAllTokens()).thenAnswer( asTokens(propKeys) );
        when(read.labelsGetAllTokens()).thenAnswer( asTokens(labels) );
        when(read.relationshipTypesGetAllTokens()).thenAnswer( asTokens(relTypes) );
        when(read.indexesGetAll()).thenAnswer( (i) -> indexes.iterator() );
        when(read.uniqueIndexesGetAll()).thenAnswer( (i) -> uniqueIndexes.iterator() );
        when(read.constraintsGetAll()).thenAnswer( (i) -> constraints.iterator() );
        when(read.proceduresGetAll() ).thenReturn( procs.getAll() );

        when(read.propertyKeyGetName( anyInt() ))
                .thenAnswer( (invocation) -> propKeys.get( (int)invocation.getArguments()[0] ) );
        when(read.labelGetName( anyInt() ))
                .thenAnswer( (invocation) -> labels.get( (int)invocation.getArguments()[0] ) );
        when(read.relationshipTypeGetName( anyInt() ))
                .thenAnswer( (invocation) -> relTypes.get( (int)invocation.getArguments()[0] ) );

        // Make it appear that labels are in use
        // TODO: We really should just have `labelsInUse()` on the Kernel API directly,
        //       it'd make testing much easier.
        when(read.constraintsGetForRelationshipType(anyInt())).thenReturn( emptyIterator() );
        when(read.indexesGetForLabel( anyInt() )).thenReturn( emptyIterator() );
        when(read.constraintsGetForLabel( anyInt() )).thenReturn( emptyIterator() );
        when(read.countsForNode( anyInt() )).thenReturn( 1L );
        when(read.countsForRelationship( anyInt(), anyInt(), anyInt() )).thenReturn( 1L );
        when(read.indexGetState( any( IndexDescriptor.class)  )).thenReturn( InternalIndexState.ONLINE );
    }

    private Answer<Iterator<Token>> asTokens( Map<Integer,String> tokens )
    {
        return (i) -> tokens.entrySet().stream()
                .map( (entry) -> new Token(entry.getValue(), entry.getKey()))
                .iterator();
    }

    private List<Object[]> call(String name, Object ... args) throws ProcedureException
    {
        CallableProcedure.BasicContext ctx = new CallableProcedure.BasicContext();
        ctx.put( KERNEL_TRANSACTION, tx );
        return Iterators.asList( procs.call( ctx, ProcedureSignature.procedureName( name.split( "\\." ) ), args ) );
    }
}
