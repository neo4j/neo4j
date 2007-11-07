package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Help extends NeoApp
{
	@Override
	public String getDescription()
	{
		return "Lists available commands";
	}
	
	@Override
	protected String exec( AppCommandParser parser, Session session, Output out )
		throws ShellException, RemoteException
	{
		out.println(  "Available commands: " +
		"cd env exit export gsh ls man mkrel mv pwd rm rmrel set quit\n" +
		"Use man <command> for info about each command." );
		return null;
	}
}
