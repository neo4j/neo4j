package org.neo4j.api.core;

import java.util.Map;
import org.neo4j.impl.core.NeoModule;
import org.neo4j.impl.event.EventModule;
import org.neo4j.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.impl.persistence.IdGeneratorModule;
import org.neo4j.impl.persistence.PersistenceModule;
import org.neo4j.impl.transaction.TxModule;

class NeoJvmInstance
{
	
	private static final String NIO_NEO_DB_CLASS = 
		"org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource";
	private static final String DEFAULT_DATA_SOURCE_NAME = 
		"nioneodb";
	
	private static boolean started = false;
	
	private static Config config = null;
	private static NioNeoDbPersistenceSource persistenceSource = null; 
	
	
	public static Config getConfig()
	{
		return config;
	}
	
	/**
	 * Starts Neo with default configuration using NioNeo DB as persistence 
	 * store. 
	 *
	 * @param storeDir path to directory where NionNeo DB store is located
	 * @param create if true a new NioNeo DB store will be created if no
	 * store exist at <CODE>storeDir</CODE>
	 * @throws StartupFailedException if unable to start
	 */
	public static void start( Class<? extends RelationshipType> clazz, 
		String storeDir, boolean create ) 
	{
		if ( started )
		{
			throw new RuntimeException( "A Neo instance already started" );
		}
//		if ( clazz.getEnumConstants() == null )
//		{
//			throw new IllegalArgumentException( "No enum constants found in " + 
//				clazz );
//		}
		
		config = new Config();
		config.getTxModule().setTxLogDirectory( storeDir );
		// create NioNeo DB persistence source
		Map<String,String> params = new java.util.HashMap<String,String>();
		params.put( "neo_store", storeDir + "/neostore" );
		params.put( "create", String.valueOf( create ) );
		params.put( "logical_log", storeDir + "/nioneo_logical.log" );
		params.put( "neostore.nodestore.db.mapped_memory", "500k" );
		params.put( "neostore.propertystore.db.mapped_memory", "1M" );
		params.put( "neostore.propertystore.db.keys.mapped_memory", "1M" );
		params.put( "neostore.propertystore.db.strings.mapped_memory", "1M" );
		params.put( "neostore.relationshipstore.db.mapped_memory", "1M" );
		byte resourceId[] = "414141".getBytes();
		config.getTxModule().registerDataSource( DEFAULT_DATA_SOURCE_NAME,
			NIO_NEO_DB_CLASS, resourceId, params );
		System.setProperty( "neo.tx_log_directory", storeDir );
		persistenceSource = new NioNeoDbPersistenceSource();
		config.setNeoPersistenceSource( DEFAULT_DATA_SOURCE_NAME, create );
		config.getNeoModule().setRelationshipTypes( clazz );
		config.getIdGeneratorModule().setPersistenceSourceInstance( 
			persistenceSource );
		config.getEventModule().init();
		config.getTxModule().init();
		config.getPersistenceModule().init();
		persistenceSource.init();
		config.getIdGeneratorModule().init();
		config.getNeoModule().init();
		
		config.getEventModule().start();
		config.getTxModule().start();
		config.getPersistenceModule().start();
		persistenceSource.start();
		config.getIdGeneratorModule().start();
		config.getNeoModule().start();
		started = true;
	}

	/**
	 * Returns true if Neo is started.
	 * 
	 * @return True if Neo started
	 */
	public static boolean started()
	{
		return started;
	}
	
	/**
	 * Shut down Neo. 
	 */
	public static void shutdown()
	{
		if ( started )
		{
			config.getNeoModule().stop();
			config.getIdGeneratorModule().stop();
			persistenceSource.stop();
			config.getPersistenceModule().stop();
			config.getTxModule().stop();
			config.getEventModule().stop();
			config.getNeoModule().destroy();
			config.getIdGeneratorModule().destroy();
			persistenceSource.destroy();
			config.getPersistenceModule().destroy();
			config.getTxModule().destroy();
			config.getEventModule().destroy();
		}
		started = false;
	}
	
	public static class Config
	{
		private EventModule eventModule;
		private TxModule txModule;
		private PersistenceModule persistenceModule;
		private boolean create = false; 
		private String persistenceSourceName;
		private IdGeneratorModule idGeneratorModule;
		private NeoModule neoModule;
		
		Config()
		{
			eventModule = new EventModule();
			txModule = new TxModule();
			persistenceModule = new PersistenceModule();
			idGeneratorModule = new IdGeneratorModule();
			neoModule = new NeoModule();
		}
		
		/**
		 * Sets the persistence source for neo to use. If this method 
		 * is never called default persistence source is used (NioNeo DB).
		 * 
		 * @param name fqn name of persistence source to use
		 */
		void setNeoPersistenceSource( String name, boolean create )
		{
			persistenceSourceName = name;
			this.create = create;
		}
		
		String getPersistenceSource()
		{
			return persistenceSourceName;
		}
		
		boolean getCreatePersistenceSource()
		{
			return create;
		}
		
		public EventModule getEventModule()
		{
			return eventModule;
		}
		
		TxModule getTxModule()
		{
			return txModule;
		}
		
		public NeoModule getNeoModule()
		{
			return neoModule;
		}
		
		PersistenceModule getPersistenceModule()
		{
			return persistenceModule;
		}
		
		IdGeneratorModule getIdGeneratorModule()
		{
			return idGeneratorModule;
		}
	}

	public static RelationshipType getRelationshipTypeByName( String name )
    {
	    return config.getNeoModule().getRelationshipTypeByName( name );
    }

	public static void addEnumRelationshipTypes( 
		Class<? extends RelationshipType> relationshipTypes )
    {
	    config.getNeoModule().addEnumRelationshipTypes( relationshipTypes );
    }

	public static Iterable<RelationshipType> getRelationshipTypes()
    {
	    return config.getNeoModule().getRelationshipTypes();
    }

	public static boolean hasRelationshipType( String name )
    {
	    return config.getNeoModule().hasRelationshipType( name );
    }

	public static RelationshipType registerRelationshipType( String name, 
		boolean create )
    {
	    return config.getNeoModule().registerRelationshipType( name, create );
    }
}