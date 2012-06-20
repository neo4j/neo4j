package org.neo4j.server.enterprise.functional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.enterprise.EnterpriseDatabase;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class EnterpriseServerIT {



	@Test
	public void shouldBeAbleToStartInHAMode() throws Throwable
	{
		// Given
		LocalhostZooKeeperCluster keeper = LocalhostZooKeeperCluster.singleton();
		File tuningFile = createNeo4jProperties(keeper);
		
		NeoServer server = EnterpriseServerBuilder.server()
			.withProperty(Configurator.DB_MODE_KEY, "HA")
			.withProperty(Configurator.DB_TUNING_PROPERTY_FILE_KEY, tuningFile.getAbsolutePath())
			.persistent()
			.build();
		
		try 
		{
			server.start();
			
			assertThat(server.getDatabase(), is(EnterpriseDatabase.class));
			assertThat(server.getDatabase().getGraph(), is(HighlyAvailableGraphDatabase.class));
		} finally {
			server.stop();
		}
	}

	private File createNeo4jProperties(LocalhostZooKeeperCluster keeper) throws IOException,
			FileNotFoundException {
		File tuningFile = File.createTempFile("neo4j-test", "properties");
		FileOutputStream fos = new FileOutputStream(tuningFile);
		try {
			Properties neo4jProps = new Properties();
			
			neo4jProps.put("ha.server_id", "1");
			neo4jProps.put("ha.coordinators", keeper.getConnectionString());
			
			neo4jProps.store(fos, "");
			return tuningFile;
		} finally {
			fos.close();
		}
	}
	
}
