<a name="0.12.0"></a>
## 0.12.0 (2014-11-16)


## Bug Fixes

* **accordion:** make header links keyboard accessible ([992b2329](angular-ui/bootstrap/commit/992b23297cd100ab4e236fba469e3a70566a4163), closes [#2869](angular-ui/bootstrap/issues/2869))
* **build:** make custom builds on demo site work ([390f2bf6](angular-ui/bootstrap/commit/390f2bf6b0846ee640e86ad87bbae8c449e53026), closes [#2960](angular-ui/bootstrap/issues/2960), [#2847](angular-ui/bootstrap/issues/2847), [#2625](angular-ui/bootstrap/issues/2625), [#2489](angular-ui/bootstrap/issues/2489), [#2357](angular-ui/bootstrap/issues/2357), [#2176](angular-ui/bootstrap/issues/2176), [#2892](angular-ui/bootstrap/issues/2892))
* **carousel:** replaced $timeout with $interval when it was wrong ([392c0ad1](angular-ui/bootstrap/commit/392c0ad13ca9b65be5e77ac0c68de24ead8ea2ce), closes [#1308](angular-ui/bootstrap/issues/1308), [#2454](angular-ui/bootstrap/issues/2454), [#2776](angular-ui/bootstrap/issues/2776))
* **datepicker:** correct button alignment when using bootstrap v3.2.0 ([460fbec7](angular-ui/bootstrap/commit/460fbec776c6d08d0e7db40aedd29d10ac48d7e9), closes [#2728](angular-ui/bootstrap/issues/2728))
* **demo:** initial load of fragment URLs ([eab6daf6](angular-ui/bootstrap/commit/eab6daf64b3c963d8e285e254c75af5f97c24ec1), closes [#2762](angular-ui/bootstrap/issues/2762))
* **dropdown:**
  * compatibility with `$location` url rewriting ([ef095170](angular-ui/bootstrap/commit/ef09517061b0b4c0c9e9f85086635af33207ec54), closes [#2343](angular-ui/bootstrap/issues/2343))
  * remove `C` restrictions to avoid conflicts ([192768e1](angular-ui/bootstrap/commit/192768e109b5c4a50c7dcd320e09d05fedd4298a), closes [#2156](angular-ui/bootstrap/issues/2156), [#2170](angular-ui/bootstrap/issues/2170))
* **tabs:**
  * make tab links keyboard accessible ([5df524b7](angular-ui/bootstrap/commit/5df524b77114bccdc9a49540e1eb52a564ee5dfd), closes [#2226](angular-ui/bootstrap/issues/2226), [#2290](angular-ui/bootstrap/issues/2290), [#2870](angular-ui/bootstrap/issues/2870), [#2304](angular-ui/bootstrap/issues/2304))
  * don't select tabs on destroy ([9939867a](angular-ui/bootstrap/commit/9939867aba0b7b763588b18829b557c052ea69ba), closes [#2155](angular-ui/bootstrap/issues/2155), [#2596](angular-ui/bootstrap/issues/2596))
* **tests:** usage of undefined variables ([34273ff0](angular-ui/bootstrap/commit/34273ff0107ecfa28438a7389d94ca619b8704e5))
* **tooltip:**
  * remove extra digest causing incompatibility ([32c4704b](angular-ui/bootstrap/commit/32c4704b748cecf2de4c651f2e5157c1ef6c88b1), closes [#2951](angular-ui/bootstrap/issues/2951), [#2959](angular-ui/bootstrap/issues/2959))
  * show correct tooltip on `ng-repeat` ([b4832c4b](angular-ui/bootstrap/commit/b4832c4b551af7e580ed65d9e5aaee1ef9e6c53e), closes [#2935](angular-ui/bootstrap/issues/2935))
  * memory leak on show/hide ([faf38d20](angular-ui/bootstrap/commit/faf38d20a49176f2016f7f7d4fa49a5c438a986e), closes [#2709](angular-ui/bootstrap/issues/2709), [#2919](angular-ui/bootstrap/issues/2919))
  * remove child scope requirement ([8204c808](angular-ui/bootstrap/commit/8204c8088139165ac9b2ad3977a2c20570e434cb), closes [#1269](angular-ui/bootstrap/issues/1269), [#2320](angular-ui/bootstrap/issues/2320), [#2203](angular-ui/bootstrap/issues/2203))
  * evaluate appendToBody on init ([e10d561f](angular-ui/bootstrap/commit/e10d561f92c2927be0ec429761fa229520fb9a51), closes [#2921](angular-ui/bootstrap/issues/2921))
  * don't use an empty transclusion fn ([689c4d01](angular-ui/bootstrap/commit/689c4d017d303b6d758164ee12837a172bb01139), closes [#2825](angular-ui/bootstrap/issues/2825))
* **typeahead:** don't leak DOM nodes ([1f6c3c92](angular-ui/bootstrap/commit/1f6c3c92af0e343c7e34b85ea6d270ac79bf6755))


## Features

* **alert:** allow alerts to be closed from a controller ([ca6fad67](angular-ui/bootstrap/commit/ca6fad675bf2aa793896bf3e086330667a5d9051), closes [#2399](angular-ui/bootstrap/issues/2399), [#2854](angular-ui/bootstrap/issues/2854))
* **typeahead:** add focus-first option ([35d0cc1d](angular-ui/bootstrap/commit/35d0cc1d57302883840f7ad54a03918ae2df001d), closes [#908](angular-ui/bootstrap/issues/908), [#2916](angular-ui/bootstrap/issues/2916))


## Breaking Changes

* `tooltip-trigger` and `popover-trigger` are no longer watched
attributes.
([a65bea95](angular-ui/bootstrap/commit/a65bea95338802b026fd213805b095b5a0b5b393))  
This affects both popovers and tooltips. The *triggers are now set up
once* and can no longer be changed after initialization.

* `dropdown` and `dropdown-toggle` are attribute-only directives. ([192768e1](angular-ui/bootstrap/commit/192768e109b5c4a50c7dcd320e09d05fedd4298a))

  Before:
    ```html
    <button class="dropdown-toggle" ...>
    ```
    After:
    ```html
    <button class="dropdown-toggle" dropdown-toggle ...>
    ```


# 0.11.2 (2014-09-26)

Revert breaking change in **dropdown** ([1a998c4](http://github.com/angular-ui/bootstrap/commit/1a998c4))

# 0.11.1 (2014-09-26)

## Features

- **modal:** 
  - add backdropClass option, similar to windowClass option ([353e6127](http://github.com/angular-ui/bootstrap/commit/353e6127))  
  - support alternative controllerAs syntax ([8d7c2a26](http://github.com/angular-ui/bootstrap/commit/8d7c2a26))  
  - allow templateUrl to be a function ([990015fb](http://github.com/angular-ui/bootstrap/commit/990015fb))   

## Bug Fixes

- **alert:** 
  - correct binding of alert type class ([aa188aec](http://github.com/angular-ui/bootstrap/commit/aa188aec))  
- **dateparser:** 
  - do not parse if no format specified ([42cc3f26](http://github.com/angular-ui/bootstrap/commit/42cc3f26))  
- **datepicker:** 
  - correct `datepicker-mode` binding for popup ([63ae06c9](http://github.com/angular-ui/bootstrap/commit/63ae06c9))  
  - memory leak fix for datepicker ([08c150e1](http://github.com/angular-ui/bootstrap/commit/08c150e1))  
- **dropdown:** 
  - close after selecting an item ([3ac3b487](http://github.com/angular-ui/bootstrap/commit/3ac3b487))  
  - remove `C` restrictions to avoid conflicts ([7512b93f](http://github.com/angular-ui/bootstrap/commit/7512b93f))  
- **modal:** 
  - allow modal.{dismiss,close} to be called again ([1590920c](http://github.com/angular-ui/bootstrap/commit/1590920c))  
  - add a work-around for transclusion scope ([0b31e865](http://github.com/angular-ui/bootstrap/commit/0b31e865))  
  - allow in-lined controller-as controllers ([79105368](http://github.com/angular-ui/bootstrap/commit/79105368))  
  - respect autofocus on child elements ([e62ab94a](http://github.com/angular-ui/bootstrap/commit/e62ab94a))  
  - controllerAs not checked ([7b7cdf84](http://github.com/angular-ui/bootstrap/commit/7b7cdf84))  
- **tabs:** 
  - remove leading newline from a template ([a708fe6d](http://github.com/angular-ui/bootstrap/commit/a708fe6d))  
- **typeahead:** 
  - timeout cancellation when deleting characters ([5dc57927](http://github.com/angular-ui/bootstrap/commit/5dc57927))  
  - allow multiple line expression ([c7db0df4](http://github.com/angular-ui/bootstrap/commit/c7db0df4))  
  - replace ng-if with ng-show in matches popup ([a0be450d](http://github.com/angular-ui/bootstrap/commit/a0be450d))   

# 0.11.0 (2014-05-01)

## Features

- **accordion:** 
  - support `is-disabled` state ([9c43ae7c](http://github.com/angular-ui/bootstrap/commit/9c43ae7c))  
- **alert:** 
  - add WAI-ARIA markup ([9a2638bf](http://github.com/angular-ui/bootstrap/commit/9a2638bf))  
- **button:** 
  - allow uncheckable radio button ([82df4fb1](http://github.com/angular-ui/bootstrap/commit/82df4fb1))  
- **carousel:** 
  - Support swipe for touchscreen devices ([85140f84](http://github.com/angular-ui/bootstrap/commit/85140f84))  
- **dateParser:** 
  - add `dateParser` service ([bd2ae0ee](http://github.com/angular-ui/bootstrap/commit/bd2ae0ee))  
- **datepicker:** 
  - add `datepicker-mode`, `init-date` & today hint ([7f4b40eb](http://github.com/angular-ui/bootstrap/commit/7f4b40eb))  
  - make widget accessible ([2423f6d4](http://github.com/angular-ui/bootstrap/commit/2423f6d4))  
  - full six-week calendar ([b0b14343](http://github.com/angular-ui/bootstrap/commit/b0b14343))  
- **dropdown:** 
  - add WAI-ARIA attributes ([22ebd230](http://github.com/angular-ui/bootstrap/commit/22ebd230))  
  - focus toggle element when opening or closing with Esc` ([f715d052](http://github.com/angular-ui/bootstrap/commit/f715d052))  
- **dropdownToggle:** 
  - support programmatic trigger & toggle callback ([ae31079c](http://github.com/angular-ui/bootstrap/commit/ae31079c))  
  - add support for `escape` key ([1417c548](http://github.com/angular-ui/bootstrap/commit/1417c548))  
- **modal:** 
  - support custom template for modal window ([96def3d6](http://github.com/angular-ui/bootstrap/commit/96def3d6))  
  - support modal window sizes ([976f6083](http://github.com/angular-ui/bootstrap/commit/976f6083))  
  - improve accessibility - add role='dialog' ([60cee9dc](http://github.com/angular-ui/bootstrap/commit/60cee9dc))  
- **pagination:** 
  - plug into `ngModel` controller ([d65901cf](http://github.com/angular-ui/bootstrap/commit/d65901cf))  
- **progressbar:** 
  - make widget accessible ([9dfe3157](http://github.com/angular-ui/bootstrap/commit/9dfe3157))  
- **rating:** 
  - plug into `ngModel` controller ([47e227f6](http://github.com/angular-ui/bootstrap/commit/47e227f6))  
  - make widget accessible ([4f56e60e](http://github.com/angular-ui/bootstrap/commit/4f56e60e))  
- **tooltip:** 
  - support more positioning options ([3704db9a](http://github.com/angular-ui/bootstrap/commit/3704db9a))  
- **typeahead:** 
  - add WAI-ARIA markup ([5ca23e97](http://github.com/angular-ui/bootstrap/commit/5ca23e97))  
  - add `aria-owns` & `aria-activedescendant` roles ([4c76a858](http://github.com/angular-ui/bootstrap/commit/4c76a858))   

## Bug Fixes

- **alert:** 
  - use interpolation for type attribute ([f0a129ad](http://github.com/angular-ui/bootstrap/commit/f0a129ad))  
  - add `alert-dismissable` class ([794954af](http://github.com/angular-ui/bootstrap/commit/794954af))  
- **carousel:** 
  - correct glyphicon ([3b6ab25b](http://github.com/angular-ui/bootstrap/commit/3b6ab25b))  
- **datepicker:** 
  - remove unneeded date creation ([68cb2e5a](http://github.com/angular-ui/bootstrap/commit/68cb2e5a))  
  - `Today` button should not set time ([e1993491](http://github.com/angular-ui/bootstrap/commit/e1993491))  
  - mark input field as invalid if the date is invalid ([467dd159](http://github.com/angular-ui/bootstrap/commit/467dd159))  
  - rename `dateFormat` to `datepickerPopup` in datepickerPopupConfig ([93da30d5](http://github.com/angular-ui/bootstrap/commit/93da30d5))  
  - parse input using dateParser ([e0eb1bce](http://github.com/angular-ui/bootstrap/commit/e0eb1bce))  
- **dropdown:**
  - use $animate for adding and removing classes ([e8d5fefc](http://github.com/angular-ui/bootstrap/commit/e8d5fefc))  
  - unbind toggle element event on scope destroy ([890e2d37](http://github.com/angular-ui/bootstrap/commit/890e2d37))  
  - do not call `on-toggle` initially ([004dd1de](http://github.com/angular-ui/bootstrap/commit/004dd1de))  
  - ensure `on-toggle` works when `is-open` is not used ([06ad3bd5](http://github.com/angular-ui/bootstrap/commit/06ad3bd5))  
- **modal:**
  - destroy modal scope after animation end ([dfc36fd9](http://github.com/angular-ui/bootstrap/commit/dfc36fd9))  
  - backdrop z-index when stacking modals ([94a7f593](http://github.com/angular-ui/bootstrap/commit/94a7f593))  
  - give a reason of rejection when escape key pressed ([cb31b875](http://github.com/angular-ui/bootstrap/commit/cb31b875))  
  - prevent default event when closing via escape key ([da951222](http://github.com/angular-ui/bootstrap/commit/da951222))
  - toggle 'modal-open' class after animation ([4d641ca7](http://github.com/angular-ui/bootstrap/commit/4d641ca7))
- **pagination:** 
  - take maxSize defaults into account ([a294c87f](http://github.com/angular-ui/bootstrap/commit/a294c87f))  
- **position:** 
  - remove deprecated body scrollTop and scrollLeft ([1ba07c1b](http://github.com/angular-ui/bootstrap/commit/1ba07c1b))  
- **progressbar:** 
  - allow fractional values for bar width ([0daa7a74](http://github.com/angular-ui/bootstrap/commit/0daa7a74))  
  - number filter in bar template and only for percent ([378a9337](http://github.com/angular-ui/bootstrap/commit/378a9337))  
- **tabs:** 
  - fire deselect before select callback ([7474c47b](http://github.com/angular-ui/bootstrap/commit/7474c47b))  
  - use interpolation for type attribute ([83ceb78a](http://github.com/angular-ui/bootstrap/commit/83ceb78a))  
  - remove `tabbable` class required for left/right tabs ([19468331](http://github.com/angular-ui/bootstrap/commit/19468331))  
- **timepicker:** 
  - evaluate correctly the `readonly-input` attribute ([f9b6c496](http://github.com/angular-ui/bootstrap/commit/f9b6c496))  
- **tooltip:** 
  - animation causes tooltip to hide on show ([2b429f5d](http://github.com/angular-ui/bootstrap/commit/2b429f5d))  
- **typeahead:** 
  - correctly handle append to body attribute ([10785736](http://github.com/angular-ui/bootstrap/commit/10785736))  
  - correctly higlight numeric matches ([09678b12](http://github.com/angular-ui/bootstrap/commit/09678b12))  
  - loading callback updates after blur ([6a830116](http://github.com/angular-ui/bootstrap/commit/6a830116))  
  - incompatibility with ng-focus ([d0024931](http://github.com/angular-ui/bootstrap/commit/d0024931))   

## Breaking Changes

- **alert:** 
 Use interpolation for type attribute.

  Before:

  ```html
  <alert type="'info'" ...></alert >
  ```
  or
  ```html
  <alert type="alert.type" ...></alert >
  ```

  After:

  ```html
  <alert type="info" ...></alert >
  ```
  or
  ```html
  <alert type="{{alert.type}}" ...></alert >
  ```

- **datepicker:** 

`show-weeks` is no longer a watched attribute
`*-format` attributes have been renamed to `format-*`
`min` attribute has been renamed to `min-date`
`max` attribute has been renamed to `max-date`

- **pagination:**

 Both `pagination` and `pager` are now integrated with `ngModelController`.
 * `page` is replaced from `ng-model`.
 * `on-select-page` is removed since `ng-change` can now be used.

  Before:
  
  ```html
  <pagination page="current" on-select-page="changed(page)" ...></pagination>
  ```
  
  After:

  ```html
  <pagination ng-model="current" ng-change="changed()" ...></pagination>
  ```
  
- **rating:** 
 `rating` is now integrated with `ngModelController`.
 * `value` is replaced from `ng-model`.

  Before:

  ```html
  <rating value="rate" ...></rating>
  ```
  
  After:
  
  ```html
  <rating ng-model="rate" ...></rating>
  ```
  
- **tabs:**

 Use interpolation for type attribute.

  Before:
  
  ```html
  <tabset type="'pills'" ...></tabset >
  <!-- or -->
  <tabset type="navtype" ...></tabset>
  ```
  
  After:
  
  ```html
  <tabset type="pills" ...></tabset>
  <!-- or -->
  <tabset type="{{navtype}}" ...></tabset>
  ```
  
# 0.10.0 (2014-01-13)

_This release adds AngularJS 1.2 support_

## Features

- **modal:** 
  - expose dismissAll on $modalStack ([bc8d21c1](http://github.com/angular-ui/bootstrap/commit/bc8d21c1))   

## Bug Fixes

- **datepicker:** 
  - evaluate `show-weeks` from `datepicker-options` ([92c1715f](http://github.com/angular-ui/bootstrap/commit/92c1715f))  
- **modal:** 
  - leaking watchers due to scope re-use ([0754ad7b](http://github.com/angular-ui/bootstrap/commit/0754ad7b))  
  - support close animation ([1933488c](http://github.com/angular-ui/bootstrap/commit/1933488c))  
- **timepicker:** 
  - add correct type for meridian button ([bcf39efe](http://github.com/angular-ui/bootstrap/commit/bcf39efe))  
- **tooltip:** 
  - performance and scope fixes ([c0df3201](http://github.com/angular-ui/bootstrap/commit/c0df3201))   

# 0.9.0 (2013-12-28)

_This release adds Bootstrap3 support_

## Features

- **accordion:** 
  - convert to bootstrap3 panel styling ([458a9bd3](http://github.com/angular-ui/bootstrap/commit/458a9bd3))  
- **carousel:** 
  - some changes for Bootstrap3 ([1f632b65](http://github.com/angular-ui/bootstrap/commit/1f632b65))  
- **collapse:** 
  - make collapse work with bootstrap3 ([517dff6e](http://github.com/angular-ui/bootstrap/commit/517dff6e))  
- **datepicker:** 
  - update to Bootstrap 3 ([37684330](http://github.com/angular-ui/bootstrap/commit/37684330))  
- **modal:** 
  - added bootstrap3 support ([444c488d](http://github.com/angular-ui/bootstrap/commit/444c488d))  
- **pagination:** 
  - support bootstrap3 ([3db699d7](http://github.com/angular-ui/bootstrap/commit/3db699d7))  
- **progressbar:** 
  - update to bootstrap3 ([5bcff623](http://github.com/angular-ui/bootstrap/commit/5bcff623))  
- **rating:** 
  - update rating to bootstrap3 ([7e60284e](http://github.com/angular-ui/bootstrap/commit/7e60284e))  
- **tabs:** 
  - add nav-justified ([3199dd88](http://github.com/angular-ui/bootstrap/commit/3199dd88))  
- **timepicker:** 
  - restyled for bootstrap 3 ([6724a721](http://github.com/angular-ui/bootstrap/commit/6724a721))  
- **typeahead:** 
  - update to Bootstrap 3 ([eadf934a](http://github.com/angular-ui/bootstrap/commit/eadf934a))   

## Bug Fixes

- **alert:** 
  - update template to Bootstrap 3 ([dfc3b0bd](http://github.com/angular-ui/bootstrap/commit/dfc3b0bd))  
- **collapse:** 
  - Prevent consecutive transitions & tidy up code ([b0032d68](http://github.com/angular-ui/bootstrap/commit/b0032d68))  
  - fixes after rebase ([dc02ad1d](http://github.com/angular-ui/bootstrap/commit/dc02ad1d))  
- **rating:** 
  - user glyhicon classes ([d221d517](http://github.com/angular-ui/bootstrap/commit/d221d517))  
- **timepicker:** 
  - fix look with bootstrap3 ([9613b61b](http://github.com/angular-ui/bootstrap/commit/9613b61b))  
- **tooltip:** 
  - re-position tooltip after draw ([a99b3608](http://github.com/angular-ui/bootstrap/commit/a99b3608))   

# 0.8.0 (2013-12-28)

## Features

- **datepicker:** 
  - option whether to display button bar in popup ([4d158e0d](http://github.com/angular-ui/bootstrap/commit/4d158e0d))  
- **modal:** 
  - add modal-open class to body on modal open ([e76512fa](http://github.com/angular-ui/bootstrap/commit/e76512fa))  
- **progressbar:** 
  - add `max` attribute & support transclusion ([365573ab](http://github.com/angular-ui/bootstrap/commit/365573ab))  
- **timepicker:** 
  - default meridian labels based on locale ([8b1ab79a](http://github.com/angular-ui/bootstrap/commit/8b1ab79a))  
- **typeahead:** 
  - add typeahead-append-to-body option ([dd8eac22](http://github.com/angular-ui/bootstrap/commit/dd8eac22))   

## Bug Fixes

- **accordion:** 
  - correct `is-open` handling for dynamic groups ([9ec21286](http://github.com/angular-ui/bootstrap/commit/9ec21286))  
- **carousel:** 
  - cancel timer on scope destruction ([5b9d929c](http://github.com/angular-ui/bootstrap/commit/5b9d929c))  
  - cancel goNext on scope destruction ([7515df45](http://github.com/angular-ui/bootstrap/commit/7515df45))  
- **collapse:** 
  - dont animate height changes from 0 to 0 ([81e014a8](http://github.com/angular-ui/bootstrap/commit/81e014a8))  
- **datepicker:** 
  - set default zero time after no date selected ([93cd0df8](http://github.com/angular-ui/bootstrap/commit/93cd0df8))  
  - fire `ngChange` on today/clear button press ([6b1c68fb](http://github.com/angular-ui/bootstrap/commit/6b1c68fb))  
  - remove datepicker's popup on scope destroy ([48955d69](http://github.com/angular-ui/bootstrap/commit/48955d69))  
  - remove edge case position updates ([1fbcb5d6](http://github.com/angular-ui/bootstrap/commit/1fbcb5d6))  
- **modal:** 
  - put backdrop in before window ([d64f4a97](http://github.com/angular-ui/bootstrap/commit/d64f4a97))  
  - grab reference to body when it is needed in lieu of when the factory is created ([dd415a98](http://github.com/angular-ui/bootstrap/commit/dd415a98))
  - focus freshly opened modal ([709e679c](http://github.com/angular-ui/bootstrap/commit/709e679c))  
  - properly animate backdrops on each modal opening ([672a557a](http://github.com/angular-ui/bootstrap/commit/672a557a))  
- **tabs:** 
  - make nested tabs work ([c9acebbe](http://github.com/angular-ui/bootstrap/commit/c9acebbe))  
- **tooltip:** 
  - update tooltip content when empty ([60515ae1](http://github.com/angular-ui/bootstrap/commit/60515ae1))  
  - support IE8 ([5dd98238](http://github.com/angular-ui/bootstrap/commit/5dd98238))  
  - unbind element events on scope destroy ([3fe7aa8c](http://github.com/angular-ui/bootstrap/commit/3fe7aa8c))  
  - respect animate attribute ([54e614a8](http://github.com/angular-ui/bootstrap/commit/54e614a8))   

## Breaking Changes

- **progressbar:** 
 The onFull/onEmpty handlers & auto/stacked types have been removed.

  To migrate your code change your markup like below.

  Before:

```html
  <progress percent="var" class="progress-warning"></progress>
```

  After:

```html
  <progressbar value="var" type="warning"></progressbar>
```

  and for stacked instead of passing array/objects you can do:

```html
  <progress><bar ng-repeat="obj in objs" value="obj.var" type="{{obj.type}}"></bar></progress>
```

# 0.7.0 (2013-11-22)

## Features

- **datepicker:** 
  - add i18n support for bar buttons in popup ([c6ba8d7f](http://github.com/angular-ui/bootstrap/commit/c6ba8d7f))  
  - dynamic date format for popup ([aa3eaa91](http://github.com/angular-ui/bootstrap/commit/aa3eaa91))  
  - datepicker-append-to-body attribute ([0cdc4609](http://github.com/angular-ui/bootstrap/commit/0cdc4609))  
- **dropdownToggle:** 
  - disable dropdown when it has the disabled class ([104bdd1b](http://github.com/angular-ui/bootstrap/commit/104bdd1b))  
- **tooltip:** 
  - add ability to enable / disable tooltip ([5d9bd058](http://github.com/angular-ui/bootstrap/commit/5d9bd058))   

## Bug Fixes

- **accordion:** 
  - assign `is-open` to correct scope ([157f614a](http://github.com/angular-ui/bootstrap/commit/157f614a))  
- **collapse:** 
  - remove element height watching ([a72c635c](http://github.com/angular-ui/bootstrap/commit/a72c635c))  
  - add the "in" class for expanded panels ([9eca35a8](http://github.com/angular-ui/bootstrap/commit/9eca35a8))  
- **datepicker:**
  - some IE8 compatibility improvements ([4540476f](http://github.com/angular-ui/bootstrap/commit/4540476f))  
  - set popup initial position in append-to-body case ([78a1e9d7](http://github.com/angular-ui/bootstrap/commit/78a1e9d7))
  - properly handle showWeeks config option ([570dba90](http://github.com/angular-ui/bootstrap/commit/570dba90))  
- **modal:** 
  - correctly close modals with no backdrop ([e55c2de3](http://github.com/angular-ui/bootstrap/commit/e55c2de3))  
- **pagination:** 
  - fix altering of current page caused by totals change ([81164dae](http://github.com/angular-ui/bootstrap/commit/81164dae))  
  - handle extreme values for `total-items` ([8ecf93ed](http://github.com/angular-ui/bootstrap/commit/8ecf93ed))  
- **position:** 
  - correct positioning for SVG elements ([968e5407](http://github.com/angular-ui/bootstrap/commit/968e5407))  
- **tabs:** 
  - initial tab selection ([a08173ec](http://github.com/angular-ui/bootstrap/commit/a08173ec))  
- **timepicker:** 
  - use html5 for input elements ([53709f0f](http://github.com/angular-ui/bootstrap/commit/53709f0f))  
- **tooltip:** 
  - restore html-unsafe compatibility with AngularJS 1.2 ([08d8b21d](http://github.com/angular-ui/bootstrap/commit/08d8b21d))  
  - hide tooltips when content becomes empty ([cf5c27ae](http://github.com/angular-ui/bootstrap/commit/cf5c27ae))  
  - tackle DOM node and event handlers leak ([0d810acd](http://github.com/angular-ui/bootstrap/commit/0d810acd))  
- **typeahead:** 
  - do not set editable error when input is empty ([006986db](http://github.com/angular-ui/bootstrap/commit/006986db))  
  - remove popup flickering ([dde804b6](http://github.com/angular-ui/bootstrap/commit/dde804b6))  
  - don't show matches if an element is not focused ([d1f94530](http://github.com/angular-ui/bootstrap/commit/d1f94530))  
  - fix loading callback when deleting characters ([0149eff6](http://github.com/angular-ui/bootstrap/commit/0149eff6))  
  - prevent accidental form submission on ENTER ([253c49ff](http://github.com/angular-ui/bootstrap/commit/253c49ff))  
  - evaluate matches source against a correct scope ([fd21214d](http://github.com/angular-ui/bootstrap/commit/fd21214d))  
  - support IE8 ([0e9f9980](http://github.com/angular-ui/bootstrap/commit/0e9f9980))   

# 0.6.0 (2013-09-08)

## Features

- **modal:** 
  - rewrite $dialog as $modal ([d7a48523](http://github.com/angular-ui/bootstrap/commit/d7a48523))  
  - add support for custom window settings ([015625d1](http://github.com/angular-ui/bootstrap/commit/015625d1))  
  - expose $close and $dismiss options on modal's scope ([8d153acb](http://github.com/angular-ui/bootstrap/commit/8d153acb))  
- **pagination:** 
  - `total-items` & optional `items-per-page` API ([e55d9063](http://github.com/angular-ui/bootstrap/commit/e55d9063))  
- **rating:** 
  - add support for custom icons per instance ([20ab01ad](http://github.com/angular-ui/bootstrap/commit/20ab01ad))  
- **timepicker:** 
  - plug into `ngModel` controller ([b08e993f](http://github.com/angular-ui/bootstrap/commit/b08e993f))   

## Bug Fixes

- **carousel:** 
  - correct reflow triggering on FFox and Safari ([d34f2de1](http://github.com/angular-ui/bootstrap/commit/d34f2de1))  
- **datepicker:** 
  - correctly manage focus without jQuery present ([d474824b](http://github.com/angular-ui/bootstrap/commit/d474824b))  
  - compatibility with angular 1.1.5 and no jquery ([bf30898d](http://github.com/angular-ui/bootstrap/commit/bf30898d))  
  - use $setViewValue for inner changes ([dd99f35d](http://github.com/angular-ui/bootstrap/commit/dd99f35d))
- **modal:**
  - insert backdrop before modal window ([d870f212](http://github.com/angular-ui/bootstrap/commit/d870f212))
  - ie8 fix after $modal rewrite ([ff9d969e](http://github.com/angular-ui/bootstrap/commit/ff9d969e))
  - opening a modal should not change default options ([82532d1b](http://github.com/angular-ui/bootstrap/commit/82532d1b))
  - backdrop should cover previously opened modals ([7fce2fe8](http://github.com/angular-ui/bootstrap/commit/7fce2fe8))
  - allow replacing object with default options ([8e7fbf06](http://github.com/angular-ui/bootstrap/commit/8e7fbf06))
- **position:**
  - fallback for IE8's scrollTop/Left for offset ([9aecd4ed](http://github.com/angular-ui/bootstrap/commit/9aecd4ed))  
- **tabs:** 
  - add DI array-style annotations ([aac4a0dd](http://github.com/angular-ui/bootstrap/commit/aac4a0dd))  
  - evaluate `vertical` on parent scope ([9af6f96e](http://github.com/angular-ui/bootstrap/commit/9af6f96e))  
- **timepicker:** 
  - add type attribute for meridian button ([1f89fd4b](http://github.com/angular-ui/bootstrap/commit/1f89fd4b))  
- **tooltip:** 
  - remove placement='mouse' option ([17163c22](http://github.com/angular-ui/bootstrap/commit/17163c22))  
- **typeahead:** 
  - fix label rendering for equal model and items names ([5de71216](http://github.com/angular-ui/bootstrap/commit/5de71216))  
  - set validity flag for non-editable inputs ([366e0c8a](http://github.com/angular-ui/bootstrap/commit/366e0c8a))  
  - plug in front of existing parsers ([80cef614](http://github.com/angular-ui/bootstrap/commit/80cef614))  
  - highlight return match if no query ([45dd9be1](http://github.com/angular-ui/bootstrap/commit/45dd9be1))  
  - keep pop-up on clicking input ([5f9e270d](http://github.com/angular-ui/bootstrap/commit/5f9e270d))  
  - remove dependency on ng-bind-html-unsafe ([75893393](http://github.com/angular-ui/bootstrap/commit/75893393))   

## Breaking Changes

- **modal:**

* `$dialog` service was refactored into `$modal`
* `modal` directive was removed - use the `$modal` service instead

Check the documentation for the `$modal` service to migrate from `$dialog`

- **pagination:** 
 API has undergone some changes in order to be easier to use.
 * `current-page` is replaced from `page`.
 * Number of pages is not defined by `num-pages`, but from `total-items` &
  `items-per-page` instead. If `items-per-page` is missing, default is 10.
 * `num-pages` still exists but is just readonly.

  Before:

```html
  <pagination num-pages="10" ...></pagination>
```

  After:

```html
  <pagination total-items="100" ...></pagination>
```

- **tooltip:** 


The placment='mouse' is gone with no equivalent
 
# 0.5.0 (2013-08-04)

## Features

- **buttons:** 
  - support dynamic true / false values in btn-checkbox ([3e30cd94](http://github.com/angular-ui/bootstrap/commit/3e30cd94))  
- **datepicker:** 
  - `ngModelController` plug & new `datepickerPopup` ([dab18336](http://github.com/angular-ui/bootstrap/commit/dab18336))  
- **rating:** 
  - added onHover and onLeave. ([5b1115e3](http://github.com/angular-ui/bootstrap/commit/5b1115e3))  
- **tabs:** 
  - added onDeselect callback, used similarly as onSelect ([fe47c9bb](http://github.com/angular-ui/bootstrap/commit/fe47c9bb))  
  - add the ability to set the direction of the tabs ([220e7b60](http://github.com/angular-ui/bootstrap/commit/220e7b60))  
- **typeahead:** 
  - support custom templates for matched items ([e2238174](http://github.com/angular-ui/bootstrap/commit/e2238174))  
  - expose index to custom templates ([5ffae83d](http://github.com/angular-ui/bootstrap/commit/5ffae83d))   

## Bug Fixes

- **datepicker:** 
  - handle correctly `min`/`max` when cleared ([566bdd16](http://github.com/angular-ui/bootstrap/commit/566bdd16))  
  - add type attribute for buttons ([25caf5fb](http://github.com/angular-ui/bootstrap/commit/25caf5fb))  
- **pagination:** 
  - handle `currentPage` number as string ([b1fa7bb8](http://github.com/angular-ui/bootstrap/commit/b1fa7bb8))  
  - use interpolation for text attributes ([f45815cb](http://github.com/angular-ui/bootstrap/commit/f45815cb))  
- **popover:** 
  - don't unbind event handlers created by other directives ([56f624a2](http://github.com/angular-ui/bootstrap/commit/56f624a2))  
  - correctly position popovers appended to body ([93a82af0](http://github.com/angular-ui/bootstrap/commit/93a82af0))  
- **rating:** 
  - evaluate `max` attribute on parent scope ([60619d51](http://github.com/angular-ui/bootstrap/commit/60619d51))  
- **tabs:** 
  - make tab contents be correctly connected to parent (#524) ([be7ecff0](http://github.com/angular-ui/bootstrap/commit/be7ecff0))  
  - Make tabset template correctly use tabset attributes (#584) ([8868f236](http://github.com/angular-ui/bootstrap/commit/8868f236))  
  - fix tab content compiling wrong (Closes #599, #631, #574) ([224bc2f5](http://github.com/angular-ui/bootstrap/commit/224bc2f5))  
  - make tabs added with active=true be selected ([360cd5ca](http://github.com/angular-ui/bootstrap/commit/360cd5ca))  
  - if tab is active at start, always select it ([ba1f741d](http://github.com/angular-ui/bootstrap/commit/ba1f741d))  
- **timepicker:** 
  - prevent date change ([ee741707](http://github.com/angular-ui/bootstrap/commit/ee741707))  
  - added wheel event to enable mousewheel on Firefox ([8dc92afa](http://github.com/angular-ui/bootstrap/commit/8dc92afa))  
- **tooltip:** 
  - fix positioning inside scrolling element ([63ae7e12](http://github.com/angular-ui/bootstrap/commit/63ae7e12))  
  - triggers should be local to tooltip instances ([58e8ef4f](http://github.com/angular-ui/bootstrap/commit/58e8ef4f))  
  - correctly handle initial events unbinding ([4fd5bf43](http://github.com/angular-ui/bootstrap/commit/4fd5bf43))  
  - bind correct 'hide' event handler ([d50b0547](http://github.com/angular-ui/bootstrap/commit/d50b0547))  
- **typeahead:** 
  - play nicelly with existing formatters ([d2df0b35](http://github.com/angular-ui/bootstrap/commit/d2df0b35))  
  - properly render initial input value ([c4e169cb](http://github.com/angular-ui/bootstrap/commit/c4e169cb))  
  - separate text field rendering and drop down rendering ([ea1e858a](http://github.com/angular-ui/bootstrap/commit/ea1e858a))  
  - fixed waitTime functionality ([90a8aa79](http://github.com/angular-ui/bootstrap/commit/90a8aa79))  
  - correctly close popup on match selection ([624fd5f5](http://github.com/angular-ui/bootstrap/commit/624fd5f5))   

## Breaking Changes

- **pagination:** 
 The 'first-text', 'previous-text', 'next-text' and 'last-text'
  attributes are now interpolated.

  To migrate your code, remove quotes for constant attributes and/or
  interpolate scope variables.

  Before:

```html
  <pagination first-text="'<<'" ...></pagination>
```
  and/or

```html
  $scope.var1 = '<<';
  <pagination first-text="var1" ...></pagination>
```
  After:

```html
  <pagination first-text="<<" ...></pagination>
```
  and/or

```html
  $scope.var1 = '<<';
  <pagination first-text="{{var1}}" ...></pagination>
```

# 0.4.0 (2013-06-24)

## Features

- **buttons:** 
  - support dynamic values in btn-radio ([e8c5b548](http://github.com/angular-ui/bootstrap/commit/e8c5b548))  
- **carousel:** 
  - add option to prevent pause ([5f895c13](http://github.com/angular-ui/bootstrap/commit/5f895c13))  
- **datepicker:** 
  - add datepicker directive ([30a00a07](http://github.com/angular-ui/bootstrap/commit/30a00a07))  
- **pagination:** 
  - option for different mode when maxSize ([a023d082](http://github.com/angular-ui/bootstrap/commit/a023d082))  
  - add pager directive ([d9526475](http://github.com/angular-ui/bootstrap/commit/d9526475))  
- **tabs:** 
  - Change directive name, add features ([c5326595](http://github.com/angular-ui/bootstrap/commit/c5326595))  
  - support disabled state ([2b78dd16](http://github.com/angular-ui/bootstrap/commit/2b78dd16))  
  - add support for vertical option ([88d17a75](http://github.com/angular-ui/bootstrap/commit/88d17a75))  
  - add support for other navigation types, like 'pills' ([53e0a39f](http://github.com/angular-ui/bootstrap/commit/53e0a39f))  
- **timepicker:** 
  - add timepicker directive ([9bc5207b](http://github.com/angular-ui/bootstrap/commit/9bc5207b))  
- **tooltip:** 
  - add mouse placement option ([ace7bc60](http://github.com/angular-ui/bootstrap/commit/ace7bc60))
  - add *-append-to-body attribute ([d0896263](http://github.com/angular-ui/bootstrap/commit/d0896263))  
  - add custom trigger support ([dfa53155](http://github.com/angular-ui/bootstrap/commit/dfa53155))  
- **typeahead:** 
  - support typeahead-on-select callback ([91ac17c9](http://github.com/angular-ui/bootstrap/commit/91ac17c9))  
  - support wait-ms option ([7f35a3f2](http://github.com/angular-ui/bootstrap/commit/7f35a3f2))   

## Bug Fixes

- **accordion:** 
  - allow accordion heading directives as attributes. ([25f6e55c](http://github.com/angular-ui/bootstrap/commit/25f6e55c))
- **carousel:** 
  - do not allow user to change slide if transitioning ([1d19663f](http://github.com/angular-ui/bootstrap/commit/1d19663f))  
  - make slide 'active' binding optional ([17d6c3b5](http://github.com/angular-ui/bootstrap/commit/17d6c3b5))  
  - fix error with deleting multiple slides at once ([3fcb70f0](http://github.com/angular-ui/bootstrap/commit/3fcb70f0))  
- **dialog:** 
  - remove dialogOpenClass to get in line with v2.3 ([f009b23f](http://github.com/angular-ui/bootstrap/commit/f009b23f))  
- **pagination:** 
  - bind *-text attributes ([e1bff6b7](http://github.com/angular-ui/bootstrap/commit/e1bff6b7))  
- **progressbar:** 
  - user `percent` attribute instead of `value`. ([58efec80](http://github.com/angular-ui/bootstrap/commit/58efec80))  
- **tooltip:** 
  - fix positioning error when appendToBody is set to true ([76fee1f9](http://github.com/angular-ui/bootstrap/commit/76fee1f9))  
  - close tooltips appended to body on location change ([041261b5](http://github.com/angular-ui/bootstrap/commit/041261b5))  
  - tooltips will hide on scope.$destroy ([3e5a58e5](http://github.com/angular-ui/bootstrap/commit/3e5a58e5))  
  - support of custom $interpolate.startSymbol ([88c94ee6](http://github.com/angular-ui/bootstrap/commit/88c94ee6))  
  - make sure tooltip scope is evicted from cache ([9246905a](http://github.com/angular-ui/bootstrap/commit/9246905a))  
- **typeahead:** 
  - return focus to the input after selecting a suggestion ([04a21e33](http://github.com/angular-ui/bootstrap/commit/04a21e33))   

## Breaking Changes

- **pagination:** 
 The 'first-text', 'previous-text', 'next-text' and 'last-text'
  attributes are now binded to parent scope.

  To migrate your code, surround the text of these attributes with quotes.

  Before:
      
    ```html
    <pagination first-text="<<"></pagination>
    ```

  After:
    
    ```html
    <pagination first-text="'<<'"></pagination>
    ```

- **progressbar:** 
 The 'value' is replaced by 'percent'.

  Before:
    
    ```html
    <progress value="..."></progress>
    ```

  After:
    
    ```html
    <progress percent="..."></progress>
    ```

- **tabs:** 
 The 'tabs' directive has been renamed to 'tabset', and
 the 'pane' directive has been renamed to 'tab'.

    To migrate your code, follow the example below.

  Before:

    ```html
    <tabs>
      <pane heading="one">
        First Content
      </pane>
      <pane ng-repeat="apple in basket" heading="{{apple.heading}}">
        {{apple.content}}
      </pane>
    </tabs>
    ```

  After:

    ```html
    <tabset>
      <tab heading="one">
        First Content
      </tab>
      <tab ng-repeat="apple in basket" heading="{{apple.heading}}">
        {{apple.content}}
      </tab>
    </tabset>
    ```

 
# 0.3.0 (2013-04-30)

## Features

- **progressbar:**
  - add progressbar directive ([261f2072](https://github.com/angular-ui/bootstrap/commit/261f2072))
- **rating:**
  - add rating directive ([6b5e6369](https://github.com/angular-ui/bootstrap/commit/6b5e6369))
- **typeahead:**
  - support the editable property ([a40c3fbe](https://github.com/angular-ui/bootstrap/commit/a40c3fbe))
  - support typeahead-loading bindable expression ([b58c9c88](https://github.com/angular-ui/bootstrap/commit/b58c9c88))
- **tooltip:**
  - added popup-delay option ([a79a2ba8](https://github.com/angular-ui/bootstrap/commit/a79a2ba8))
  - added appendToBody to $tooltip ([1ee467f8](https://github.com/angular-ui/bootstrap/commit/1ee467f8))
  - added tooltip-html-unsafe directive ([45ed2805](https://github.com/angular-ui/bootstrap/commit/45ed2805))
  - support for custom triggers ([b1ba821b](https://github.com/angular-ui/bootstrap/commit/b1ba821b))

## Bug Fixes

- **alert:**
  - don't show close button if no close callback specified ([c2645f4a](https://github.com/angular-ui/bootstrap/commit/c2645f4a))
- **carousel:**
  - Hide navigation indicators if only one slide ([aedc0565](https://github.com/angular-ui/bootstrap/commit/aedc0565))
- **collapse:**
  - remove reference to msTransition for IE10 ([55437b16](https://github.com/angular-ui/bootstrap/commit/55437b16))
- **dialog:**
  - set _open to false on init ([dcc9ef31](https://github.com/angular-ui/bootstrap/commit/dcc9ef31))
  - close dialog on location change ([474ce52e](https://github.com/angular-ui/bootstrap/commit/474ce52e))
  - IE8 fix to not set data() against text nodes ([a6c540e5](https://github.com/angular-ui/bootstrap/commit/a6c540e5))
  - fix $apply in progres on $location change ([77e6acb9](https://github.com/angular-ui/bootstrap/commit/77e6acb9))
- **tabs:**
  - remove superfluous href from tabs template ([38c1badd](https://github.com/angular-ui/bootstrap/commit/38c1badd))
- **tooltip:**
  - fix positioning issues in tooltips and popovers ([6458f487](https://github.com/angular-ui/bootstrap/commit/6458f487))
- **typeahead:**
  - close matches popup on click outside typeahead ([acca7dcd](https://github.com/angular-ui/bootstrap/commit/acca7dcd))
  - stop keydown event propagation when ESC pressed to discard matches ([22a00cd0](https://github.com/angular-ui/bootstrap/commit/22a00cd0))
  - correctly render initial model value ([929a46fa](https://github.com/angular-ui/bootstrap/commit/929a46fa))
  - correctly higlight matches if query contains regexp-special chars ([467afcd6](https://github.com/angular-ui/bootstrap/commit/467afcd6))
  - fix matches pop-up positioning issues ([74beecdb](https://github.com/angular-ui/bootstrap/commit/74beecdb))

# 0.2.0 (2013-03-03)

## Features

- **dialog:**
  - Make $dialog 'resolve' property to work the same way of $routeProvider.when ([739f86f](https://github.com/angular-ui/bootstrap/commit/739f86f))
- **modal:**
  - allow global override of modal options ([acaf72b](https://github.com/angular-ui/bootstrap/commit/acaf72b))
- **buttons:**
  - add checkbox and radio buttons ([571ccf4](https://github.com/angular-ui/bootstrap/commit/571ccf4))
- **carousel:**
  - add slide indicators ([3b677ee](https://github.com/angular-ui/bootstrap/commit/3b677ee))
- **typeahead:**
  - add typeahead directive ([6a97da2](https://github.com/angular-ui/bootstrap/commit/6a97da2))
- **accordion:**
  - enable HTML in accordion headings ([3afcaa4](https://github.com/angular-ui/bootstrap/commit/3afcaa4))
- **pagination:**
  - add first/last link & constant congif options ([0ff0454](https://github.com/angular-ui/bootstrap/commit/0ff0454))

## Bug fixes

- **dialog:**
  - update resolve section to new syntax ([1f87486](https://github.com/angular-ui/bootstrap/commit/1f87486))
  - $compile entire modal ([7575b3c](https://github.com/angular-ui/bootstrap/commit/7575b3c))
- **tooltip:**
  - don't show tooltips if there is no content to show ([030901e](https://github.com/angular-ui/bootstrap/commit/030901e))
  - fix placement issues ([a2bbf4d](https://github.com/angular-ui/bootstrap/commit/a2bbf4d))
- **collapse:**
  - Avoids fixed height on collapse ([ff5d119](https://github.com/angular-ui/bootstrap/commit/ff5d119))
- **accordion:**
  - fix minification issues ([f4da4d6](https://github.com/angular-ui/bootstrap/commit/f4da4d6))
- **typeahead:**
  -  update inputs value on mapping where label is not derived from the model ([a5f64de](https://github.com/angular-ui/bootstrap/commit/a5f64de))

# 0.1.0 (2013-02-02)

_Very first, initial release_.

## Features

Version `0.1.0` was released with the following directives:

* accordion
* alert
* carousel
* dialog
* dropdownToggle
* modal
* pagination
* popover
* tabs
* tooltip
