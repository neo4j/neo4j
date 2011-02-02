/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * Naive implementation for escaping html strings. Should not be relied on for security.
 */
wa.htmlEscape = function( text ) {
	
	return wa.replaceAll(text, [
	    [/&/g,"&amp;"],
	    [/</g,"&lt;"],
	    [/>/g,"&gt;"],
	    [/"/g,"&quot;"],
        [/ /g,"&nbsp;"],
	    [/'/g,"&#x27;"],
	    [/\//g,"&#x2F;"]]);
	
};

/**
 * Replace all occurrences of a list of items.
 * @param string to be escaped
 * @param array of two-item arrays that defines replacements. For example:
 * [
 *   ['a','b'],
 *   ['c','a']
 * ]
 * 
 * The first character signifies what to replace, the second what to put there instead.
 */
wa.replaceAll = function( text, replacements ) {
	
	for(var i=0,l=replacements.length; i<l; i++) {
		text = text.replace(replacements[i][0], replacements[i][1]);
	}
	
	return text;
	 
};