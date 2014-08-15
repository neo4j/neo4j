We are always looking for the quality contributions and will be happy to accept your Pull Requests as long as those adhere to some basic rules:

* Please make sure that your contribution fits well in the project's context:
  * we are aiming at rebuilding bootstrap directives in pure AngularJS, without any dependencies on any external JavaScript library;
  * the only dependency should be bootstrap CSS and its markup structure;
  * directives should be html-agnostic as much as possible which in practice means:
        * templates should be referred to using the `templateUrl` property
        * it should be easy to change a default template to a custom one
        * directives shouldn't manipulate DOM structure directly (when possible)
* Please assure that you are submitting quality code, specifically make sure that:
  * your directive has accompanying tests and all the tests are passing; don't hesitate to contact us (angular-ui@googlegroups.com) if you need any help with unit testing
  * your PR doesn't break the build; check the Travis-CI build status after opening a PR and push corrective commits if anything goes wrong
