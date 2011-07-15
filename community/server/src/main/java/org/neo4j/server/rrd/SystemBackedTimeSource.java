package org.neo4j.server.rrd;

public class SystemBackedTimeSource implements TimeSource {

	@Override
	public long getTime() {
		return System.currentTimeMillis();
	}

}
