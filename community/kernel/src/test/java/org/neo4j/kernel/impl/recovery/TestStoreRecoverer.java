/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.recovery;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;


public class TestStoreRecoverer {

	@Test
	public void shouldNotWantToRecoverIntactStore() throws Exception
	{
		File store = null;
		try
		{
			store = createIntactStore();
			
			StoreRecoverer recoverer = new StoreRecoverer();
			
			assertThat(recoverer.recoveryNeededAt(store, new HashMap<String,String>()), is(false));
			
		} finally 
		{
			if(store != null)
			{
				FileUtils.deleteRecursively(store);
			}
		}
		
	}
	
	@Test
	public void shouldWantToRecoverBrokenStore() throws Exception
	{
		File store = null;
		try
		{
			store = createIntactStore();
			new File(store,"nioneo_logical.log.active").delete();
			
			StoreRecoverer recoverer = new StoreRecoverer();
			
			assertThat(recoverer.recoveryNeededAt(store, new HashMap<String,String>()), is(true));
			
		} finally 
		{
			if(store != null)
			{
				FileUtils.deleteRecursively(store);
			}
		}
		
	}

	@Test
	public void shouldBeAbleToRecoverBrokenStore() throws Exception
	{
		File store = null;
		try
		{
			store = createIntactStore();
			new File(store,"nioneo_logical.log.active").delete();
			
			StoreRecoverer recoverer = new StoreRecoverer();
			
			assertThat(recoverer.recoveryNeededAt(store, new HashMap<String,String>()), is(true));
			
			recoverer.recover(store, new HashMap<String,String>());
			
			assertThat(recoverer.recoveryNeededAt(store, new HashMap<String,String>()), is(false));
			
		} finally 
		{
			if(store != null)
			{
				FileUtils.deleteRecursively(store);
			}
		}
		
	}

	private File createIntactStore() throws IOException {
		File tmpFile = File.createTempFile( "neo4j-test", "" );
        tmpFile.delete();
		GraphDatabaseService db = 
				new GraphDatabaseFactory().newEmbeddedDatabase(tmpFile.getCanonicalPath());
		
		db.shutdown();
		return tmpFile;
	}
	
}
