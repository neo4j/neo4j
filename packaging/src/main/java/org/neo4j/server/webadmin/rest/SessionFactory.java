package org.neo4j.server.webadmin.rest;

import org.neo4j.server.database.Database;
import org.neo4j.server.webadmin.console.ScriptSession;

public interface SessionFactory
{
    ScriptSession createSession(String engineName, Database database);
}
