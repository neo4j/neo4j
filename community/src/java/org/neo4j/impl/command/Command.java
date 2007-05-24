package org.neo4j.impl.command;

/**
 * A command encapsulates information about some type of operation that can
 * be executed and undone. A command can only be executed once and undone 
 * once after it has been executed. Ex:
 * <p>
 * <CODE><pre>
 *	MyCommand cmd = null;
 *	try
 *	{
 *		cmd = // create command
 *		cmd.setupDoThisOperation(...); // ... 
 *		cmd.execute(); // command will invoke onExecute()
 *		// ...
 *	}
 *	catch ( ExecuteFailedException e )
 *	{
 *		// error undo...
 *		cmd.undo(); // command will invoke onUndo()
 *	}
 * </pre></CODE>
 * <p>
 * NOTE: This command framework is designed to participate in transactions. If 
 * a transaction fail all commands executed during that transaction will 
 * automatically be undone. Using this framework for other purposes may not 
 * work as expected since commands are not ordered hierarchically. That is if 
 * command A is executed and the execution results in that a new command B is
 * created and executed, invoking A.undo() will not result in B being undone.
 */
public abstract class Command
{
	private boolean isExecuted = false;
	private boolean isUndone = false;
	
	protected Command()
	{
	}
	
	/**
	 * Adds <CODE>this</CODE> command to the transaction.
	 */
	protected void addCommandToTransaction()
	{
		CommandManager.getManager().addCommandToTransaction( this );
	}
	
	/**
	 * Executes this command performing the desired operation.
	 *
	 * @throws ExecuteFailedException if command fails.
	 */
	public synchronized final void execute() throws ExecuteFailedException
	{
		// we're only allowed to invoke execute once
		if ( !isExecuted && !isUndone )
		{
			onExecute(); 
			isExecuted = true; 			
		}
		
	}
	
	/**
	 * Undo the executed operation. This method should reverse all the
	 * changes made by execute. Just to clarify, it has to reverse the
	 * changes made in "memory" by execute or memory and persistence storage 
	 * may be inconsistent.
	 */
	public synchronized final void undo()
	{
		// we're only allowed to invoke undo once and 
		// only after executed has been invoked
		if ( isExecuted && !isUndone )
		{
			isUndone = true;
			onUndo();
		}
	}
	
	// called by CommandPool when a command is released back into pool
	synchronized void reset()
	{
		isExecuted = false;
		isUndone = false;
		onReset();
	}
	
	/**
	 * This is where the execute implementation specific code goes. Called when 
	 * <CODE>execute()</CODE> is invoked.
	 */
	protected abstract void onExecute() throws ExecuteFailedException;

	/**
	 * This is where the undo implementation specific code goes. Called when 
	 * <CODE>undo()</CODE> is invoked.
	 */
	protected abstract void onUndo();

	/**
	 * This is where the reset implementation specific code goes. Called when 
	 * <CODE>reset()</CODE> is invoked. This method should put the command
	 * in such state it can be reused.
	 */
	protected abstract void onReset();
}