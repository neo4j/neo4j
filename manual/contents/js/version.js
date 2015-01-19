window.neo4jVersion="${project.version}";

(function (){
  var pathParts = window.location.pathname.split( '/' );
  var doc = pathParts[pathParts.length - 1];
  if ( doc === '' || doc === 'index.html' )
  {
    window.neo4jPageId = 'index';
  }
  else
  {
    window.neo4jPageId = doc.substr( 0, doc.length - 5 );
  }
})();
