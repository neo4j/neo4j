/**
 * Start webadmin.
 */
$( function()
{
    $.jTemplatesDebugMode(false);
    
    // Load UI
    wa.ui.MainMenu.init();
    wa.ui.Pages.init();
    wa.ui.Helpers.init();

    // Trigger init event
    wa.trigger( "init" );
    
} );