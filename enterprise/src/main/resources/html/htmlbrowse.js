var Neo4jHtmlBrowse = function()
{
    this.start = function()
    {
        shortenLinks();
    };

    this.getRelationships = getRelationships;
    this.search = search;

    function shortenLinks()
    {
        var loc = window.location;
        var hostString = loc.protocol + "//" + loc.hostname;
        if ( ( loc.protocol === "http:" && loc.port != 80 )
                || ( loc.protocol === "https:" && loc.port != 443 ) )
        {
            hostString += ':' + loc.port;
        }
        hostString += "/";
        var rootElement = document.getElementById( "page-body" );
        var links = rootElement.getElementsByTagName( "a" );
        var i = 0, link
        while ( link = links[i++] )
        {
            var href = link.getAttribute( "href" );
            if ( href.substr( 0, hostString.length ) === hostString )
            {
                var textNode = link.firstChild;
                if ( textNode.nodeType === 3 )
                {
                    textNode.nodeValue = textNode.nodeValue
                            .substring( hostString.length - 1 );
                }
            }
        }
    }

    function getRelationships()
    {
        var options = document.getElementById( "types" ).options;
        var typesString = '';
        for ( var i = 0; i < options.length; i++ )
        {
            if ( !options[i].selected ) continue;
            if ( typesString.length > 0 ) typesString += "&";
            typesString += options[i].value
        }
        var dir = document.getElementById( "direction" );
        var loc = dir.options[dir.selectedIndex].value;
        loc = loc.replace( /\{.*types\}/, typesString );
        window.location = loc;
    }

    function search( template, keyId, valueId )
    {
        var key = document.getElementById( keyId ).value;
        var value = document.getElementById( valueId ).value;
        if ( key.length === 0 || value.length === 0 )
        {
            alert( "Both key and value must be entered to search the index." );
            return;
        }
        template = template.replace( /\{key\}/, key );
        template = template.replace( /\{value\}/, value );
        window.location = template;
    }
};

var neo4jHtmlBrowse = new Neo4jHtmlBrowse();
