package org.neo4j.util.shell;

/**
 * Whether or not options are expected to be supplied with a value.
 */
public enum OptionValueType
{
	/**
	 * No value is to be specified.
	 */
	NONE,
	
	/**
	 * There may be a value supplied.
	 */
	MAY,
	
	/**
	 * There must be a supplied value.
	 */
	MUST,
}
