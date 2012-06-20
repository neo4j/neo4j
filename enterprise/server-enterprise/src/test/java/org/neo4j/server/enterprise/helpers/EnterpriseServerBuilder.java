package org.neo4j.server.enterprise.helpers;

import static org.neo4j.server.ServerTestUtils.createTempDir;

import java.io.File;
import java.io.IOException;

import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.EphemeralDatabase;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.helpers.ServerBuilder;
import org.neo4j.server.rest.paging.LeaseManagerProvider;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;

public class EnterpriseServerBuilder extends ServerBuilder {

    public static EnterpriseServerBuilder server()
    {
        return new EnterpriseServerBuilder();
    }
	
	@Override
	public EnterpriseNeoServer build() throws IOException
    {
        if ( dbDir == null )
        {
            this.dbDir = createTempDir().getAbsolutePath();
        }
        File configFile = createPropertiesFiles();
        
        if ( startupHealthCheck == null )
        {
            startupHealthCheck = new StartupHealthCheck()
            {
                @Override
				public boolean run()
                {
                    return true;
                }
            };
        }

        if ( clock != null )
        {
            LeaseManagerProvider.setClock( clock );
        }

        return new EnterpriseNeoServer(new PropertyFileConfigurator( new Validator( new DatabaseLocationMustBeSpecifiedRule() ), configFile ))
	    {
        	@Override
        	protected StartupHealthCheck createHealthCheck() {
        		return startupHealthCheck;
        	}

        	@Override
        	protected Database createDatabase() {
        		return persistent ? 
        				super.createDatabase() :
    					new EphemeralDatabase(configurator.configuration());
        		
        	}
	    };
    }
	
}
