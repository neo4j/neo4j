package org.neo4j.impl.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import org.neo4j.api.core.NeoService;
import org.neo4j.impl.shell.apps.Ls;
import org.neo4j.util.shell.AbstractClient;
import org.neo4j.util.shell.BashVariableInterpreter;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellServer;
import org.neo4j.util.shell.SimpleAppServer;
import org.neo4j.util.shell.BashVariableInterpreter.Replacer;
import org.neo4j.util.shell.apps.Help;

public class NeoShellServer extends SimpleAppServer
{
	private NeoService neo;
	private BashVariableInterpreter bashInterpreter;
	
	public NeoShellServer( NeoService neo ) 
		throws RemoteException
	{
		super();
		this.addPackage( Ls.class.getPackage().getName() );
		this.neo = neo;
		this.bashInterpreter = new BashVariableInterpreter();
		this.bashInterpreter.addReplacer( "W", new WorkingDirReplacer() );
		this.setProperty( AbstractClient.PROMPT_KEY, "neo-sh \\W$ " );
	}
	
	@Override
	public String welcome()
	{
		return "Welcome to NeoShell\n" +
			Help.getHelpString( this );
	}
	
	@Override
	public Serializable interpretVariable( String key, Serializable value,
		Session session ) throws RemoteException
	{
		if ( key.equals( AbstractClient.PROMPT_KEY ) )
		{
			return this.bashInterpreter.interpret( ( String ) value,
				this, session );
		}
		return value;
	}
	
	public NeoService getNeo()
	{
		return this.neo;
	}
	
	public static class WorkingDirReplacer implements Replacer
	{
		public String getReplacement( ShellServer server, Session session )
		{
			return "" + NeoApp.getDisplayNameForNode(
				NeoApp.getCurrentNode( session ) );
		}
	}
}
