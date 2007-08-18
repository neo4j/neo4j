package org.neo4j.impl.nioneo.xa;

import java.io.IOException;

public interface PropertyIndexEventConsumer
{
	public void createPropertyIndex( int id, String key ) 
		throws IOException;
	
	public String getKeyFor( int id ) throws IOException;
}
