/*
if( !QUnit.urlParams.storage ){
	localStorage.setItem( "refreshTest", "true" );
	document.location.href = document.location.href + mark + "storage=true";
}


test("proper loading after page refesh", function(){
	
	
});
*/

test("localStorage setup", function(){
	
	ok( localStorage , "localStorage exists" );	
	
});