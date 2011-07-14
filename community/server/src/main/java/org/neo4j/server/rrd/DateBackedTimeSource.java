package org.neo4j.server.rrd;

import java.util.Date;

public class DateBackedTimeSource implements TimeSource {

	@Override
	public long getTime() {
		return new Date().getTime();
	}

}
