package org.neo4j.util.shell;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ClassLister
{
	public static <T> Collection<Class<? extends T>>
		listClassesExtendingOrImplementing( Class<T> superClass,
		Collection<String> lookInThesePackages )
	{
		String classPath = System.getProperty( "java.class.path" );
		StringTokenizer tokenizer = new StringTokenizer( classPath,
			File.pathSeparator );
		Collection<Class<? extends T>> classes =
			new HashSet<Class<? extends T>>();
		while ( tokenizer.hasMoreTokens() )
		{
			collectClasses( classes, superClass, lookInThesePackages,
				tokenizer.nextToken() );
		}
		return Collections.unmodifiableCollection( classes );
	}
	
	private static <T> void collectClasses(
		Collection<Class<? extends T>> classes, Class<T> superClass,
		Collection<String> lookInThesePackages, String classPathToken )
	{
		File directory = new File( classPathToken );
		if ( !directory.exists() )
		{
			return;
		}
		
		try
		{
			if ( directory.isDirectory() )
			{
				// TODO
			}
			else
			{
				JarFile jarFile = new JarFile( directory );
				Enumeration<JarEntry> entries = jarFile.entries();
				while ( entries.hasMoreElements() )
				{
					JarEntry entry = entries.nextElement();
					String entryName = fixJarEntryClassName( entry.getName() );
					tryCollectClass( classes, superClass, lookInThesePackages,
						entryName );
				}
			}
		}
		catch ( IOException e )
		{
			throw new RuntimeException( "Error collecting classes from " +
				classPathToken, e );
		}
	}
	
	private static String fixJarEntryClassName( String entryName )
	{
		entryName = entryName.replace( File.separatorChar, '.' );
		String ending = ".class";
		if ( entryName.endsWith( ending ) )
		{
			entryName = entryName.substring( 0,
				entryName.length() - ending.length() );
		}
		return entryName;
	}
	
	private static <T> void tryCollectClass(
		Collection<Class<? extends T>> classes, Class<T> superClass,
		Collection<String> lookInThesePackages, String className )
	{
		try
		{
			if ( !classNameIsInPackage( className, lookInThesePackages ) )
			{
				return;
			}
			
			Class<? extends T> cls = ( Class<? extends T> )
				Class.forName( className );
			if ( cls.isInterface() ||
				Modifier.isAbstract( cls.getModifiers() ) )
			{
				return;
			}
			if ( superClass.isAssignableFrom( cls ) )
			{
				classes.add( cls );
			}
		}
		catch ( Throwable e )
		{
			// Ok
		}
	}
	
	private static boolean classNameIsInPackage( String className,
		Collection<String> lookInThesePackages )
	{
		if ( lookInThesePackages == null )
		{
			return true;
		}
		String packageName = packageForClassName( className );
		return lookInThesePackages.contains( packageName );
	}
	
	private static String packageForClassName( String className )
	{
		int index = className.lastIndexOf( '.' );
		if ( index == -1 )
		{
			return className;
		}
		return className.substring( 0, index );
	}
}
