/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.impl;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.core.TestNeo;

public abstract class AbstractNeoTestCase extends TestCase
{
    public AbstractNeoTestCase( String testName )
    {
        super( testName );
    }

    private NeoService neo;
    private Transaction tx;

    public NeoService getNeo()
    {
        return neo;
    }

    public EmbeddedNeo getEmbeddedNeo()
    {
        return (EmbeddedNeo) neo;
    }

    public Transaction getTransaction()
    {
        return tx;
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite( TestNeo.class );
        return suite;
    }

    public void setUp()
    {
        neo = new EmbeddedNeo( "var/neo-test" );
        tx = neo.beginTx();
    }

    public void tearDown()
    {
        tx.finish();
        neo.shutdown();
    }

    public void setTransaction( Transaction tx )
    {
        this.tx = tx;
    }

    public void newTransaction()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
        }
        tx = neo.beginTx();
    }
    
    public void commit()
    {
        if ( tx != null )
        {
            tx.success();
            tx.finish();
        }
    }
    
    public NodeManager getNodeManager()
    {
        return ((EmbeddedNeo) neo).getConfig().getNeoModule().getNodeManager();
    }
}