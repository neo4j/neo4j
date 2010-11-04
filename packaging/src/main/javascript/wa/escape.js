/**
 * Naive implementation for escaping html strings. Should not be relied on for security.
 */
wa.htmlEscape = function( text ) {
	
	return wa.replaceAll(text, [
	    [/&/g,"&amp;"],
	    [/</g,"&lt;"],
	    [/>/g,"&gt;"],
	    [/"/g,"&quot"],
	    [/'/g,"&#x27"],
	    [/\//g,"&#x2F"]]);
	
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