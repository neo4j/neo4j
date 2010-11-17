package org.neo4j.server.rrd;

import com.sun.jersey.api.core.HttpContext;
import org.neo4j.server.database.AbstractInjectableProvider;
import org.rrd4j.core.RrdDb;

import javax.ws.rs.ext.Provider;

@Provider
public class RrdDbProvider extends AbstractInjectableProvider<RrdDb>
{
	private RrdDb rrdDb;

	public RrdDbProvider(RrdDb rrdDb)
	{
		super(RrdDb.class);
		this.rrdDb = rrdDb;
	}

	@Override
	public RrdDb getValue(HttpContext c)
	{
		return rrdDb;
	}
}
