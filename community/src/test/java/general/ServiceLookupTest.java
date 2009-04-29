package general;

import java.io.File;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.api.core.NeoService;
import org.neo4j.remote.RemoteNeo;
import org.neo4j.remote.sites.LocalSite;
import org.neo4j.remote.sites.RmiSite;

public class ServiceLookupTest
{
	private static final String PATH = "target/neo";
	private static final String RMI_RESOURCE = "rmi://localhost/"
		+ ServiceLookupTest.class.getSimpleName();
	private static boolean rmi = true;
	
	@BeforeClass
	public static void setUp()
	{
		try
		{
			LocateRegistry.createRegistry( Registry.REGISTRY_PORT );
		}
		catch ( RemoteException e )
		{
			e.printStackTrace();
			rmi = false;
		}
	}

	@Test
	public void testLocalSite() throws Exception
	{
		NeoService neo = new RemoteNeo(
				"file://" + new File(PATH).getAbsolutePath() );
		neo.shutdown();
	}

	@Test
	public void testRmiSite() throws Exception
	{
		Assume.assumeTrue( setupRmi() );
		NeoService neo = new RemoteNeo( RMI_RESOURCE );
		neo.shutdown();
	}
	
	private static boolean setupRmi() throws Exception
	{
		try
		{
			RmiSite.register( new LocalSite( PATH ), RMI_RESOURCE );
		}
		catch ( ConnectException ex )
		{
			if ( rmi )
			{
				throw ex;
			}
			else
			{
				return false;
			}
		}
		return true;
	}

	@Ignore( "Not implemented" ) @Test
	public void testTcpSite() throws Exception
	{
		// TODO: set up server
		NeoService neo = new RemoteNeo( "tcp://localhost" );
		neo.shutdown();
		// TODO: shut down server
	}
}
