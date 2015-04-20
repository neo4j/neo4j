/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.impl;

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

/**
 * Well, since Class#getExtendingClasses doesn't exist, use this instead.
 * @author Mattias Persson
 */
public class ClassLister
{
	private static final String CLASS_NAME_ENDING = ".class";
	
	/**
	 * @param <T> the class type.
	 * @param superClass the class which the resulting classes must implement
	 * or extend.
	 * @param lookInThesePackages an optional collection of which java packages
	 * to search in. If null is specified then all packages are searched.
	 * @return all classes (in the class path) which extends or implements
	 * a certain class.
	 */
	public static <T> Collection<Class<? extends T>>
		listClassesExtendingOrImplementing( Class<? extends T> superClass,
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
		Collection<Class<? extends T>> classes, Class<? extends T> superClass,
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
				collectFromDirectory( classes, superClass, lookInThesePackages,
					"", directory );
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
	
	private static <T> void collectFromDirectory(
		Collection<Class<? extends T>> classes, Class<? extends T> superClass,
		Collection<String> lookInThesePackages, String prefix, File directory )
	{
		// Should we even bother walking through these directories?
		// Check with lookInThesePackages if there's a package matching this.
		boolean botherToBrowseFurtherDown = false;
		if ( lookInThesePackages != null )
		{
			for ( String packageName : lookInThesePackages )
			{
				if ( packageName.startsWith( prefix ) )
				{
					botherToBrowseFurtherDown = true;
					break;
				}
			}
		}
		else
		{
			botherToBrowseFurtherDown = true;
		}
		if ( !botherToBrowseFurtherDown )
		{
			return;
		}
		
		File[] files = directory.listFiles();
		if ( files == null )
		{
			return;
		}
		
		for ( File file : files )
		{
			if ( file.isDirectory() )
			{
				collectFromDirectory( classes, superClass, lookInThesePackages,
					addToPrefix( prefix, file.getName() ), file );
			}
			else
			{
				String className = addToPrefix( prefix, file.getName() );
				className = trimFromClassEnding( className );
				tryCollectClass( classes, superClass, lookInThesePackages,
					className );
			}
		}
	}
	
	private static String addToPrefix( String prefix, String toAdd )
	{
		prefix = prefix == null ? "" : prefix;
		if ( prefix.length() > 0 )
		{
			prefix += ".";
		}
		prefix += toAdd;
		return prefix;
	}
	
	private static String trimFromClassEnding( String className )
	{
		if ( className.endsWith( CLASS_NAME_ENDING ) )
		{
			className = className.substring( 0,
				className.length() - CLASS_NAME_ENDING.length() );
		}
		return className;
	}
	
	private static String fixJarEntryClassName( String entryName )
	{
		entryName = entryName.replace( File.separatorChar, '.' );
		entryName = entryName.replace( '/', '.' );
		return trimFromClassEnding( entryName );
	}
	
	private static <T> void tryCollectClass(
		Collection<Class<? extends T>> classes, Class<? extends T> superClass,
		Collection<String> lookInThesePackages, String className )
	{
		try
		{
			if ( !classNameIsInPackage( className, lookInThesePackages ) )
			{
				return;
			}
			
			Class<? extends T> cls = Class.forName( className ).asSubclass(
				superClass );
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
