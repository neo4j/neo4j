package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.App;
import org.neo4j.util.shell.CommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Gsh extends NeoApp
{
	private App sh;
	
	public Gsh()
	{
		this.sh = new org.neo4j.util.shell.apps.extra.Gsh();
	}
	
	@Override
	public String getDescription()
	{
		return this.sh.getDescription();
	}

	@Override
	public String getDescription( String option )
	{
		return this.sh.getDescription( option );
	}

	@Override
	protected String exec( CommandParser parser, Session session, Output out )
		throws ShellException, RemoteException
	{
		return sh.execute( parser, session, out );
	}
}
