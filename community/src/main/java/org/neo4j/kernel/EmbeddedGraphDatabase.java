/*
 * Copyright (c) 2002-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel;

import java.io.Serializable;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.transaction.TransactionFailureException;

/**
 * An implementation of {@link GraphDatabaseService} that is used to embed Neo4j
 * in an application. You typically instantiate it by invoking the
 * {@link #EmbeddedGraphDatabase(String) single argument constructor} that takes
 * a path to a directory where Neo4j will store its data files, as such:
 * 
 * <pre>
 * <code>
 * GraphDatabaseService graphDb = new EmbeddedGraphDatabase( &quot;var/graphdb&quot; );
 * // ... use Neo4j
 * graphDb.shutdown();
 * </code>
 * </pre>
 * 
 * For more information, see {@link GraphDatabaseService}.
 */
public final class EmbeddedGraphDatabase implements GraphDatabaseService
{
    private final EmbeddedGraphDbImpl neoImpl;

    /**
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already exist.
     * 
     * @param storeDir the store directory for the Neo4j store files
     */
    public EmbeddedGraphDatabase( String storeDir )
    {
        this.neoImpl = new EmbeddedGraphDbImpl( storeDir, this );
    }

    /**
     * A non-standard way of creating an embedded {@link GraphDatabaseService}
     * with a set of configuration parameters. Will most likely be removed in
     * future releases.
     * <p>
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already exist.
     * 
     * @param storeDir the store directory for the db files
     * @param params configuration parameters
     */
    public EmbeddedGraphDatabase( String storeDir, Map<String,String> params )
    {
        this.neoImpl = new EmbeddedGraphDbImpl( storeDir, params, this );
    }

    /**
     * A non-standard convenience method that loads a standard property file and
     * converts it into a generic <Code>Map<String,String></CODE>. Will most
     * likely be removed in future releases.
     * 
     * @param file the property file to load
     * @return a map containing the properties from the file
     * @throws IllegalArgumentException if file does not exist
     */
    public static Map<String,String> loadConfigurations( String file )
    {
        return EmbeddedGraphDbImpl.loadConfigurations( file );
    }

    public Node createNode()
    {
        return neoImpl.createNode();
    }

    public Node getNodeById( long id )
    {
        return neoImpl.getNodeById( id );
    }

    public Relationship getRelationshipById( long id )
    {
        return neoImpl.getRelationshipById( id );
    }

    public Node getReferenceNode()
    {
        return neoImpl.getReferenceNode();
    }

    public void shutdown()
    {
        neoImpl.shutdown();
    }

    public boolean enableRemoteShell()
    {
        return neoImpl.enableRemoteShell();
    }

    public boolean enableRemoteShell(
        final Map<String,Serializable> initialProperties )
    {
        return neoImpl.enableRemoteShell( initialProperties );
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return neoImpl.getRelationshipTypes();
    }

    /**
     * @throws TransactionFailureException if unable to start transaction
     */
    public Transaction beginTx()
    {
        return neoImpl.beginTx();
    }

    /**
     * Returns a non-standard configuration object. Will most likely be removed
     * in future releases.
     * 
     * @return a configuration object
     */
    public Config getConfig()
    {
        return neoImpl.getConfig();
    }

    @Override
    public String toString()
    {
        return super.toString() + " [" + neoImpl.getStoreDir() + "]";
    }

    public String getStoreDir()
    {
        return neoImpl.getStoreDir();
    }

    public Iterable<Node> getAllNodes()
    {
        return neoImpl.getAllNodes();
    }
}