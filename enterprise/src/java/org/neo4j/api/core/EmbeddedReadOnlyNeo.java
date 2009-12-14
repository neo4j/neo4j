/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.api.core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public final class EmbeddedReadOnlyNeo implements NeoService
{
    private static Map<String,String> readOnlyParams = 
        new HashMap<String,String>();

    static 
    {
        readOnlyParams.put( "read_only", "true" );
    };
    
    private final EmbeddedNeoImpl neoImpl; 

    public EmbeddedReadOnlyNeo( String storeDir )
    {
        this( storeDir, readOnlyParams );
    }

    public EmbeddedReadOnlyNeo( String storeDir, Map<String,String> params )
    {
        params.put( "read_only", "true" );
        this.neoImpl = new EmbeddedNeoImpl( storeDir, params, this );
    }

    /**
     * A non-standard Convenience method that loads a standard property file and
     * converts it into a generic <Code>Map<String,String></CODE>. Will most 
     * likely be removed in future releases.
     * @param file the property file to load
     * @return a map containing the properties from the file
     */
    public static Map<String,String> loadConfigurations( String file )
    {
        return EmbeddedNeoImpl.loadConfigurations( file );
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#createNode()
     */
    public Node createNode()
    {
        return neoImpl.createNode();
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getNodeById(long)
     */
    public Node getNodeById( long id )
    {
        return neoImpl.getNodeById( id );
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getRelationshipById(long)
     */
    public Relationship getRelationshipById( long id )
    {
        return neoImpl.getRelationshipById( id );
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getReferenceNode()
     */
    public Node getReferenceNode()
    {
        return neoImpl.getReferenceNode();
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#shutdown()
     */
    public void shutdown()
    {
        neoImpl.shutdown();
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#enableRemoteShell()
     */
    public boolean enableRemoteShell()
    {
        return neoImpl.enableRemoteShell();
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#enableRemoteShell(java.util.Map)
     */
    public boolean enableRemoteShell(
        final Map<String,Serializable> initialProperties )
    {
        return neoImpl.enableRemoteShell( initialProperties );
    }

    /*
     * (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getRelationshipTypes()
     */
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