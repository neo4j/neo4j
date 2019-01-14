/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.builtinprocs;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProcedureResourcesIT
{
    @Rule
    public DatabaseRule db = new EnterpriseDatabaseRule()
            .withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

    private final String indexDefinition = ":Label(prop)";
    private final String explicitIndexName = "explicitIndex";
    private final String relExplicitIndexName = "relExplicitIndex";
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
        // when
        createExplicitIndex();
        createIndex();
        for ( ProcedureSignature procedure : db.getDependencyResolver().resolveDependency( Procedures.class ).getAllProcedures() )
        {
            // then
            initialData();
            ProcedureData procedureData = null;
            try
            {
                procedureData = procedureDataFor( procedure );
                verifyProcedureCloseAllAcquiredKernelStatements( procedureData );
            }
            catch ( Exception e )
            {
                throw new Exception( "Failed on procedure: \"" + procedureData + "\"", e );
            }
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

    private void verifyProcedureCloseAllAcquiredKernelStatements( ProcedureData proc ) throws ExecutionException, InterruptedException
    {
        if ( proc.skip )
        {
            return;
        }
        String failureMessage = "Failed on procedure " + proc.name;
        try ( Transaction outer = db.beginTx() )
        {
            String procedureQuery = proc.buildProcedureQuery();
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

    private void createExplicitIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.index().forNodes( explicitIndexName );
            db.index().forRelationships( relExplicitIndexName );
            tx.success();
        }
    }

    private void clearDb()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "MATCH (n) DETACH DELETE n" ).close();
            tx.success();
        }
    }

    private static class ProcedureData
    {
        private final String name;
        private final List<Object> params = new ArrayList<>();
        private String setupQuery;
        private String postQuery;
        private boolean skip;

        private ProcedureData( ProcedureSignature procedure )
        {
            this.name = procedure.name().toString();
        }

        private void withParam( Object param )
        {
            this.params.add( param );
        }

        private void withSetup( String setupQuery, String postQuery )
        {
            this.setupQuery = setupQuery;
            this.postQuery = postQuery;
        }

        private String buildProcedureQuery()
        {
            StringJoiner stringJoiner = new StringJoiner( ",", "CALL " + name + "(", ")" );
            for ( Object parameter : params )
            {
                stringJoiner.add( parameter.toString() );
            }
            if ( setupQuery != null && postQuery != null )
            {
                return setupQuery + " " + stringJoiner.toString() + " " + postQuery;
            }
            else
            {
                return stringJoiner.toString();
            }
        }

        @Override
        public String toString()
        {
            return buildProcedureQuery();
        }
    }

    private ProcedureData procedureDataFor( ProcedureSignature procedure )
    {
        ProcedureData proc = new ProcedureData( procedure );
        switch ( proc.name )
        {
        case "db.createProperty":
            proc.withParam( "'propKey'" );
            break;
        case "db.resampleIndex":
            proc.withParam( "'" + indexDefinition + "'" );
            break;
        case "db.createRelationshipType":
            proc.withParam( "'RelType'" );
            break;
        case "dbms.queryJmx":
            proc.withParam( "'*:*'" );
            break;
        case "db.awaitIndex":
            proc.withParam( "'" + indexDefinition + "'" );
            proc.withParam( 100 );
            break;
        case "db.createLabel":
            proc.withParam( "'OtherLabel'" );
            break;
        case "dbms.killQuery":
            proc.withParam( "'query-1234'" );
            break;
        case "dbms.killQueries":
            proc.withParam( "['query-1234']" );
            break;
        case "dbms.setTXMetaData":
            proc.withParam( "{realUser:'MyMan'}" );
            break;
        case "dbms.listActiveLocks":
            proc.withParam( "'query-1234'" );
            break;
        case "db.index.explicit.seekNodes":
            proc.withParam( "'" + explicitIndexName + "'" );
            proc.withParam( "'noKey'" );
            proc.withParam( "'noValue'" );
            break;
        case "db.index.explicit.searchNodes":
            proc.withParam( "'" + explicitIndexName + "'" );
            proc.withParam( "'noKey:foo*'" );
            break;
        case "db.index.explicit.searchRelationships":
            proc.withParam( "'" + relExplicitIndexName + "'" );
            proc.withParam( "'noKey:foo*'" );
            break;
        case "db.index.explicit.searchRelationshipsIn":
            proc.withParam( "'" + relExplicitIndexName + "'" );

            proc.withParam( "n" );
            proc.withParam( "'noKey:foo*'" );
            proc.withSetup( "OPTIONAL MATCH (n) WITH n LIMIT 1", "YIELD relationship AS r RETURN r" );
            break;
        case "db.index.explicit.searchRelationshipsOut":
            proc.withParam( "'" + relExplicitIndexName + "'" );
            proc.withParam( "n" );
            proc.withParam( "'noKey:foo*'" );
            proc.withSetup( "OPTIONAL MATCH (n) WITH n LIMIT 1", "YIELD relationship AS r RETURN r" );
            break;
        case "db.index.explicit.searchRelationshipsBetween":
            proc.withParam( "'" + relExplicitIndexName + "'" );
            proc.withParam( "n" );
            proc.withParam( "n" );
            proc.withParam( "'noKey:foo*'" );
            proc.withSetup( "OPTIONAL MATCH (n) WITH n LIMIT 1", "YIELD relationship AS r RETURN r" );
            break;
        case "db.index.explicit.seekRelationships":
            proc.withParam( "'" + relExplicitIndexName + "'" );
            proc.withParam( "'noKey'" );
            proc.withParam( "'noValue'" );
            break;
        case "db.index.explicit.auto.seekNodes":
            proc.withParam( "'noKey'" );
            proc.withParam( "'noValue'" );
            break;
        case "db.index.explicit.auto.searchNodes":
            proc.withParam( "'noKey:foo*'" );
            break;
        case "db.index.explicit.auto.searchRelationships":
            proc.withParam( "'noKey:foo*'" );
            break;
        case "db.index.explicit.auto.seekRelationships":
            proc.withParam( "'noKey'" );
            proc.withParam( "'noValue'" );
            break;
        case "db.index.explicit.existsForNodes":
            proc.withParam( "'" + explicitIndexName + "'" );
            break;
        case "db.index.explicit.existsForRelationships":
            proc.withParam( "'" + explicitIndexName + "'" );
            break;
        case "db.index.explicit.forNodes":
            proc.withParam( "'" + explicitIndexName + "'" );
            break;
        case "db.index.explicit.forRelationships":
            proc.withParam( "'" + explicitIndexName + "'" );
            break;
        case "db.index.explicit.addNode":
            proc.withParam( "'" + explicitIndexName + "'" );
            proc.withParam( "n" );
            proc.withParam( "'prop'" );
            proc.withParam( "'value'");
            proc.withSetup( "OPTIONAL MATCH (n) WITH n LIMIT 1", "YIELD success RETURN success" );
            break;
        case "db.index.explicit.addRelationship":
            proc.withParam( "'" + explicitIndexName + "'" );
            proc.withParam( "r" );
            proc.withParam( "'prop'" );
            proc.withParam( "'value'");
            proc.withSetup( "OPTIONAL MATCH ()-[r]->() WITH r LIMIT 1", "YIELD success RETURN success" );
            break;
        case "db.index.explicit.removeNode":
            proc.withParam( "'" + explicitIndexName + "'" );
            proc.withParam( "n" );
            proc.withParam( "'prop'" );
            proc.withSetup( "OPTIONAL MATCH (n) WITH n LIMIT 1", "YIELD success RETURN success" );
            break;
        case "db.index.explicit.removeRelationship":
            proc.withParam( "'" + explicitIndexName + "'" );
            proc.withParam( "r" );
            proc.withParam( "'prop'" );
            proc.withSetup( "OPTIONAL MATCH ()-[r]->() WITH r LIMIT 1", "YIELD success RETURN success" );
            break;
        case "db.index.explicit.drop":
            proc.withParam( "'" + explicitIndexName + "'" );
            break;
        case "dbms.setConfigValue":
            proc.withParam( "'dbms.logs.query.enabled'" );
            proc.withParam( "'false'" );
            break;
        case "db.createIndex":
            proc.withParam( "':Person(name)'" );
            proc.withParam( "'lucene+native-2.0'" );
            break;
        case "db.createNodeKey":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        case "db.createUniquePropertyConstraint":
            // Grabs schema lock an so can not execute concurrently with node creation
            proc.skip = true;
            break;
        default:
        }
        return proc;
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
