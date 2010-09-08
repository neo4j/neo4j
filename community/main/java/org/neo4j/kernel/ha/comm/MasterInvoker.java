package org.neo4j.kernel.ha.comm;

import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.Response;

public interface MasterInvoker
{
    Response<DataWriter> invoke( Master master );
}
