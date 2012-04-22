/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples.osgi;

import java.util.ArrayList;
import java.util.Properties;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.index.lucene.LuceneIndexProvider;
import org.neo4j.kernel.ListIndexIterable;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.SoftCacheProvider;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class Neo4jActivator implements BundleActivator
{

    private static GraphDatabaseService db;
    private ServiceRegistration serviceRegistration;
    private ServiceRegistration indexServiceRegistration;

    @Override
    public void start( BundleContext context ) throws Exception
    {
        System.out.print("Opening database in embedded mode: ");
        ArrayList<CacheProvider> cacheList = new ArrayList<CacheProvider>();
        cacheList.add( new SoftCacheProvider() );
        IndexProvider lucene = new LuceneIndexProvider();
        ArrayList<IndexProvider> provs = new ArrayList<IndexProvider>();
        provs.add( lucene );
        ListIndexIterable providers = new ListIndexIterable();
        providers.setIndexProviders( provs );
        GraphDatabaseFactory gdbf = new GraphDatabaseFactory();
        gdbf.setIndexProviders( providers );
        gdbf.setCacheProviders( cacheList );
        db = gdbf.newEmbeddedDatabase( "target/db");
              System.out.println("created database, db="+db);
        serviceRegistration = context.registerService( GraphDatabaseService.class.getName(), db, new Properties() );
        System.out.println("registered " + serviceRegistration.getReference());
        indexServiceRegistration = context.registerService( Index.class.getName(), db.index().forNodes( "nodes" ), new Properties() );
        System.out.println("registered " + indexServiceRegistration.getReference());
        Transaction tx = db.beginTx();
        try {
            System.out.print("Populating it ... ");
            Node firstNode = db.createNode();
            Node secondNode = db.createNode();
            Relationship relationship = firstNode.createRelationshipTo( secondNode, DynamicRelationshipType.withName( "KNOWS" ) );

            firstNode.setProperty( "message", "Hello, " );
            secondNode.setProperty( "message", "world!" );
            relationship.setProperty( "message", "brave Neo4j " );
            db.index().forNodes( "nodes" ).add( firstNode, "message", "Hello" );
            System.out.println("OK, db="+db);

            System.out.print( firstNode.getProperty( "message" ) );
            System.out.print( relationship.getProperty( "message" ) );
            System.out.println( secondNode.getProperty( "message" ) );

            tx.success();
        } 
        catch (Exception e) {
            System.out.println("KO: " + e.getMessage());
        } 
        finally {
            tx.finish();
        }

 
    }

    @Override
    public void stop( BundleContext context ) throws Exception
    {
        System.out.print("Closing database: ");
        serviceRegistration.unregister();
        indexServiceRegistration.unregister();
        db.shutdown();
        System.out.println("OK");

    }

}
