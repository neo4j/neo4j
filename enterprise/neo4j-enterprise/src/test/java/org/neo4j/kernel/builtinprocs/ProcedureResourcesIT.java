/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.builtinprocs;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProcedureResourcesIT
{
    @Rule
    public DatabaseRule db = new EnterpriseDatabaseRule();

    private final String indexDefinition = ":Label(prop)";
    private final String legacyIndexName = "legacyIndex";
    private final String relLegacyIndexName = "relLegacyIndex";
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @After
    public void tearDown() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 5, TimeUnit.SECONDS );
    }

    @Test
    public void allProcedures() throws Exception
    {
        // given
        Map<String,List<Object>> allProceduresWithParameters = allProceduresWithParameters();

        // when
        createLegacyIndex();
        createIndex();
        for ( Map.Entry<String,List<Object>> procedureWithParams : allProceduresWithParameters.entrySet() )
        {
            // then
            initialData();
            verifyProcedureCloseAllAcquiredKernelStatements( procedureWithParams.getKey(), procedureWithParams.getValue() );
            clearDb();
        }
    }

    private void initialData()
    {
        Label unusedLabel = Label.label( "unusedLabel" );
        RelationshipType unusedRelType = RelationshipType.withName( "unusedRelType" );
        String unusedPropKey = "unusedPropKey";
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode( unusedLabel );
            node1.setProperty( unusedPropKey, "value" );
            Node node2 = db.createNode( unusedLabel );
            node2.setProperty( unusedPropKey, 1 );
            node1.createRelationshipTo( node2, unusedRelType );
            tx.success();
        }
    }

    private void verifyProcedureCloseAllAcquiredKernelStatements( String procedureName, List<Object> parameters )
            throws ExecutionException, InterruptedException
    {
        String failureMessage = "Failed on procedure " + procedureName;
        try ( Transaction outer = db.beginTx() )
        {
            String procedureQuery = buildProcedureQuery( procedureName, parameters );
            exhaust( db.execute( procedureQuery ) ).close();
            exhaust( db.execute( "MATCH (mo:Label) WHERE mo.prop = 'n/a' RETURN mo" ) ).close();
            executeInOtherThread( "CREATE(mo:Label) SET mo.prop = 'val' RETURN mo" );
            Result result = db.execute( "MATCH (mo:Label) WHERE mo.prop = 'val' RETURN mo" );
            assertTrue( failureMessage, result.hasNext() );
            Map<String,Object> next = result.next();
            assertNotNull( failureMessage, next.get( "mo" ) );
            exhaust( result );
            result.close();
            outer.success();
        }
    }

    private Result exhaust( Result execute )
    {
        while ( execute.hasNext() )
        {
            execute.next();
        }
        return execute;
    }

    private void createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CREATE INDEX ON " + indexDefinition );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 5, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private void createLegacyIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( legacyIndexName );
            db.index().forRelationships( relLegacyIndexName );
            tx.success();
        }
    }

    private String buildProcedureQuery( String procedureName, List<Object> parameters )
    {
        StringJoiner stringJoiner = new StringJoiner( ",", "CALL " + procedureName + "(", ")" );
        for ( Object parameter : parameters )
        {
            stringJoiner.add( parameter.toString() );
        }
        return stringJoiner.toString();
    }

    private void clearDb()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "MATCH (n) DETACH DELETE n" ).close();
            tx.success();
        }
    }

    private Map<String,List<Object>> allProceduresWithParameters()
    {
        Set<ProcedureSignature> allProcedures = db.getDependencyResolver().resolveDependency( Procedures.class ).getAllProcedures();
        Map<String,List<Object>> allProceduresWithParameters = new HashMap<>();
        for ( ProcedureSignature procedure : allProcedures )
        {
            List<Object> params = paramsFor( procedure );
            allProceduresWithParameters.put( procedure.name().toString(), params );
        }
        return allProceduresWithParameters;
    }

    private List<Object> paramsFor( ProcedureSignature procedure )
    {
        List<Object> parameters = new ArrayList<>();
        switch ( procedure.name().toString() )
        {
        case "db.createProperty":
            parameters.add( "'propKey'" );
            break;
        case "db.resampleIndex":
            parameters.add( "'" + indexDefinition + "'" );
            break;
        case "db.createRelationshipType":
            parameters.add( "'RelType'" );
            break;
        case "dbms.queryJmx":
            parameters.add( "'*:*'" );
            break;
        case "db.awaitIndex":
            parameters.add( "'" + indexDefinition + "'" );
            parameters.add( 100 );
            break;
        case "db.createLabel":
            parameters.add( "'OtherLabel'" );
            break;
        case "dbms.killQuery":
            parameters.add( "'query-1234'" );
            break;
        case "dbms.killQueries":
            parameters.add( "['query-1234']" );
            break;
        case "dbms.setTXMetaData":
            parameters.add( "{realUser:'MyMan'}" );
            break;
        case "dbms.listActiveLocks":
            parameters.add( "'query-1234'" );
            break;
        case "db.nodeManualIndexSeek":
            parameters.add( "'" + legacyIndexName + "'" );
            parameters.add( "'noKey'" );
            parameters.add( "'noValue'" );
            break;
        case "db.nodeManualIndexSearch":
            parameters.add( "'" + legacyIndexName + "'" );
            parameters.add( "'noKey:foo*'" );
            break;
        case "db.relationshipManualIndexSearch":
            parameters.add( "'" + relLegacyIndexName + "'" );
            parameters.add( "'noKey:foo*'" );
            break;
        case "db.relationshipManualIndexSeek":
            parameters.add( "'" + relLegacyIndexName + "'" );
            parameters.add( "'noKey'" );
            parameters.add( "'noValue'" );
            break;
        case "dbms.setConfigValue":
            parameters.add( "'dbms.logs.query.enabled'" );
            parameters.add( "'false'" );
            break;
        default:
        }
        return parameters;
    }

    private void executeInOtherThread( String query ) throws ExecutionException, InterruptedException
    {
        Future<?> future = executor.submit( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                exhaust( db.execute( query ) );
                tx.success();
            }
        } );
        future.get();
    }

}
