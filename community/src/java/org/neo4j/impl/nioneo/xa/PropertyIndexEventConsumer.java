package org.neo4j.impl.nioneo.xa;

import java.io.IOException;
import org.neo4j.impl.core.RawPropertyIndex;

public interface PropertyIndexEventConsumer
{
	public void createPropertyIndex( int id, String key ) 
		throws IOException;
	
	public String getKeyFor( int id ) throws IOException;

	public RawPropertyIndex[] getPropertyIndexes( int count ) 
		throws IOException;
}
