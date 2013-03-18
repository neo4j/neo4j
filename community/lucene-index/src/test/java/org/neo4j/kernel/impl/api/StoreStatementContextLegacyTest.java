package org.neo4j.kernel.impl.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.ImpermanentGraphDatabase;

public class StoreStatementContextLegacyTest
{
    @Test
    public void should_know_about_existence_of_legacy_node_indexes() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        String indexName = "foo";
        db.index().forNodes( indexName ).add( db.createNode(), "key", "value1" );
        tx.success();
        tx.finish();

        // WHEN & THEN
        assertTrue( statement.hasLegacyNodeIndex( indexName ) );
        assertFalse( statement.hasLegacyNodeIndex( "bar" ) );
    }

    @Test
    public void should_know_about_existence_of_legacy_rel_indexes() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        String indexName = "foo";
        Relationship relationship = db.createNode().createRelationshipTo( db.createNode(), withName( "RELATED" ) );
        db.index().forRelationships( indexName ).add( relationship, "key", "value1" );
        tx.success();
        tx.finish();

        // WHEN & THEN
        assertTrue( statement.hasLegacyRelationshipIndex( indexName ) );
        assertFalse( statement.hasLegacyRelationshipIndex( "bar" ) );
    }

    private GraphDatabaseAPI db;
    private StatementContext statement;

    @Before
    public void before()
    {
        db = new ImpermanentGraphDatabase();
        IndexingService indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        @SuppressWarnings("deprecation")// Ooh, jucky
                NeoStoreXaDataSource neoStoreDataSource = db.getDependencyResolver()
                .resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource();
        statement = new StoreStatementContext(
                db.getDependencyResolver().resolveDependency( PropertyIndexManager.class ),
                db.getDependencyResolver().resolveDependency( PersistenceManager.class ),
                db.getDependencyResolver().resolveDependency( NodeManager.class ),
                neoStoreDataSource.getNeoStore(),
                indexingService, new IndexReaderFactory.Caching( indexingService ),
                db.getDependencyResolver().resolveDependency( IndexManager.class ) );
    }

    @After
    public void after()
    {
        db.shutdown();
    }
}
