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
import java.util.Hashtable;
import java.util.List;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.lucene.LuceneKernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.SoftCacheProvider;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

// START SNIPPET: setup
public class Neo4jActivator implements BundleActivator
{
    private static GraphDatabaseService db;
    private ServiceRegistration serviceRegistration;
    private ServiceRegistration indexServiceRegistration;

    @Override
    public void start( BundleContext context ) throws Exception
    {
        //the cache providers
        ArrayList<CacheProvider> cacheList = new ArrayList<CacheProvider>();
        cacheList.add( new SoftCacheProvider() );

        //the kernel extensions
        LuceneKernelExtensionFactory lucene = new LuceneKernelExtensionFactory();
        List<KernelExtensionFactory<?>> extensions = new ArrayList<KernelExtensionFactory<?>>();
        extensions.add( lucene );

        //the database setup
        GraphDatabaseFactory gdbf = new GraphDatabaseFactory();
        gdbf.setKernelExtensions( extensions );
        gdbf.setCacheProviders( cacheList );
        db = gdbf.newEmbeddedDatabase( "target/db" );

        //the OSGi registration
        serviceRegistration = context.registerService(
                GraphDatabaseService.class.getName(), db, new Hashtable<String, String>() );
        System.out.println( "registered " + serviceRegistration.getReference() );
        indexServiceRegistration = context.registerService(
                Index.class.getName(), db.index().forNodes( "nodes" ),
                new Hashtable<String, String>() );
        try ( Transaction tx = db.beginTx() )
        {
            Node firstNode = db.createNode();
            Node secondNode = db.createNode();
            Relationship relationship = firstNode.createRelationshipTo(
                    secondNode, DynamicRelationshipType.withName( "KNOWS" ) );

            firstNode.setProperty( "message", "Hello, " );
            secondNode.setProperty( "message", "world!" );
            relationship.setProperty( "message", "brave Neo4j " );
            db.index().forNodes( "nodes" ).add( firstNode, "message", "Hello" );
            tx.success();
        }
    }

    @Override
    public void stop( BundleContext context ) throws Exception
    {
        serviceRegistration.unregister();
        indexServiceRegistration.unregister();
        db.shutdown();
    }
}
// END SNIPPET: setup
