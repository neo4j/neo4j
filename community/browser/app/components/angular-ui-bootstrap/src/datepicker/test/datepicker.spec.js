describe('datepicker directive', function () {
  var $rootScope, $compile, element;
  beforeEach(module('ui.bootstrap.datepicker'));
  beforeEach(module('template/datepicker/datepicker.html'));
  beforeEach(module('template/datepicker/day.html'));
  beforeEach(module('template/datepicker/month.html'));
  beforeEach(module('template/datepicker/year.html'));
  beforeEach(module('template/datepicker/popup.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.date = new Date('September 30, 2010 15:30:00');
  }));

  function getTitle() {
    return element.find('th').eq(1).find('button').first().text();
  }

  function clickTitleButton() {
    element.find('th').eq(1).find('button').first().click();
  }

  function clickPreviousButton(times) {
    var el = element.find('th').eq(0).find('button').eq(0);
    for (var i = 0, n = times || 1; i < n; i++) {
      el.click();
    }
  }

  function clickNextButton() {
    element.find('th').eq(2).find('button').eq(0).click();
  }

  function getLabelsRow() {
    return element.find('thead').find('tr').eq(1);
  }

  function getLabels() {
    var els = getLabelsRow().find('th'),
        labels = [];
    for (var i = 1, n = els.length; i < n; i++) {
      labels.push( els.eq(i).text() );
    }
    return labels;
  }

  function getWeeks() {
    var rows = element.find('tbody').find('tr'),
        weeks = [];
    for (var i = 0, n = rows.length; i < n; i++) {
      weeks.push( rows.eq(i).find('td').eq(0).first().text() );
    }
    return weeks;
  }

  function getOptions( dayMode ) {
    var tr = element.find('tbody').find('tr');
    var rows = [];

    for (var j = 0, numRows = tr.length; j < numRows; j++) {
      var cols = tr.eq(j).find('td'), days = [];
      for (var i = dayMode ? 1 : 0, n = cols.length; i < n; i++) {
        days.push( cols.eq(i).find('button').text() );
      }
      rows.push(days);
    }
    return rows;
  }

  function clickOption( index ) {
    getAllOptionsEl().eq(index).click();
  }

  function getAllOptionsEl( dayMode ) {
    return element.find('tbody').find('button');
  }

  function expectSelectedElement( index ) {
    var buttons = getAllOptionsEl();
    angular.forEach( buttons, function( button, idx ) {
      expect(angular.element(button).hasClass('btn-info')).toBe( idx === index );
    });
  }

  function triggerKeyDown(element, key, ctrl) {
    var keyCodes = {
      'enter': 13,
      'space': 32,
      'pageup': 33,
      'pagedown': 34,
      'end': 35,
      'home': 36,
      'left': 37,
      'up': 38,
      'right': 39,
      'down': 40,
      'esc': 27
    };
    var e = $.Event('keydown');
    e.which = keyCodes[key];
    if (ctrl) {
      e.ctrlKey = true;
    }
    element.trigger(e);
  }

  describe('', function () {
    beforeEach(function() {
      element = $compile('<datepicker ng-model="date"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('is has a `<table>` element', function() {
      expect(element.find('table').length).toBe(1);
    });

    it('shows the correct title', function() {
      expect(getTitle()).toBe('September 2010');
    });

    it('shows the label row & the correct day labels', function() {
      expect(getLabelsRow().css('display')).not.toBe('none');
      expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
    });

    it('renders the calendar days correctly', function() {
      expect(getOptions(true)).toEqual([
        ['29', '30', '31', '01', '02', '03', '04'],
        ['05', '06', '07', '08', '09', '10', '11'],
        ['12', '13', '14', '15', '16', '17', '18'],
        ['19', '20', '21', '22', '23', '24', '25'],
        ['26', '27', '28', '29', '30', '01', '02'],
        ['03', '04', '05', '06', '07', '08', '09']
      ]);
    });

    it('renders the week numbers based on ISO 8601', function() {
      expect(getWeeks()).toEqual(['34', '35', '36', '37', '38', '39']);
    });

    it('value is correct', function() {
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
    });

    it('has `selected` only the correct day', function() {
      expectSelectedElement( 32 );
    });

    it('has no `selected` day when model is cleared', function() {
      $rootScope.date = null;
      $rootScope.$digest();

      expect($rootScope.date).toBe(null);
      expectSelectedElement( null );
    });

    it('does not change current view when model is cleared', function() {
      $rootScope.date = null;
      $rootScope.$digest();

      expect($rootScope.date).toBe(null);
      expect(getTitle()).toBe('September 2010');
    });

    it('`disables` visible dates from other months', function() {
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).find('span').hasClass('text-muted')).toBe( index < 3 || index > 32 );
      });
    });

    it('updates the model when a day is clicked', function() {
      clickOption( 17 );
      expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
    });

    it('moves to the previous month & renders correctly when `previous` button is clicked', function() {
      clickPreviousButton();

      expect(getTitle()).toBe('August 2010');
      expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
      expect(getOptions(true)).toEqual([
        ['01', '02', '03', '04', '05', '06', '07'],
        ['08', '09', '10', '11', '12', '13', '14'],
        ['15', '16', '17', '18', '19', '20', '21'],
        ['22', '23', '24', '25', '26', '27', '28'],
        ['29', '30', '31', '01', '02', '03', '04'],
        ['05', '06', '07', '08', '09', '10', '11']
      ]);

      expectSelectedElement( null, null );
    });

    it('updates the model only when a day is clicked in the `previous` month', function() {
      clickPreviousButton();
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));

      clickOption( 17 );
      expect($rootScope.date).toEqual(new Date('August 18, 2010 15:30:00'));
    });

    it('moves to the next month & renders correctly when `next` button is clicked', function() {
      clickNextButton();

      expect(getTitle()).toBe('October 2010');
      expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
      expect(getOptions(true)).toEqual([
        ['26', '27', '28', '29', '30', '01', '02'],
        ['03', '04', '05', '06', '07', '08', '09'],
        ['10', '11', '12', '13', '14', '15', '16'],
        ['17', '18', '19', '20', '21', '22', '23'],
        ['24', '25', '26', '27', '28', '29', '30'],
        ['31', '01', '02', '03', '04', '05', '06']
      ]);

      expectSelectedElement( 4 );
    });

    it('updates the model only when a day is clicked in the `next` month', function() {
      clickNextButton();
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));

      clickOption( 17 );
      expect($rootScope.date).toEqual(new Date('October 13, 2010 15:30:00'));
    });

    it('updates the calendar when a day of another month is selected', function() {
      clickOption( 33 );
      expect($rootScope.date).toEqual(new Date('October 01, 2010 15:30:00'));
      expect(getTitle()).toBe('October 2010');
      expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
      expect(getOptions(true)).toEqual([
        ['26', '27', '28', '29', '30', '01', '02'],
        ['03', '04', '05', '06', '07', '08', '09'],
        ['10', '11', '12', '13', '14', '15', '16'],
        ['17', '18', '19', '20', '21', '22', '23'],
        ['24', '25', '26', '27', '28', '29', '30'],
        ['31', '01', '02', '03', '04', '05', '06']
      ]);

      expectSelectedElement( 5 );
    });

    // issue #1697
    it('should not "jump" months', function() {
      $rootScope.date = new Date('January 30, 2014');
      $rootScope.$digest();
      clickNextButton();
      expect(getTitle()).toBe('February 2014');
      clickPreviousButton();
      expect(getTitle()).toBe('January 2014');
    });

    describe('when `model` changes', function () {
      function testCalendar() {
        expect(getTitle()).toBe('November 2005');
        expect(getOptions(true)).toEqual([
          ['30', '31', '01', '02', '03', '04', '05'],
          ['06', '07', '08', '09', '10', '11', '12'],
          ['13', '14', '15', '16', '17', '18', '19'],
          ['20', '21', '22', '23', '24', '25', '26'],
          ['27', '28', '29', '30', '01', '02', '03'],
          ['04', '05', '06', '07', '08', '09', '10']
        ]);

        expectSelectedElement( 8 );
      }

      describe('to a Date object', function() {
        it('updates', function() {
          $rootScope.date = new Date('November 7, 2005 23:30:00');
          $rootScope.$digest();
          testCalendar();
          expect(angular.isDate($rootScope.date)).toBe(true);
        });

        it('to a date that is invalid, it gets invalid', function() {
          $rootScope.date = new Date('pizza');
          $rootScope.$digest();
          expect(element.hasClass('ng-invalid')).toBeTruthy();
          expect(element.hasClass('ng-invalid-date')).toBeTruthy();
          expect(angular.isDate($rootScope.date)).toBe(true);
          expect(isNaN($rootScope.date)).toBe(true);
        });
      });

      describe('not to a Date object', function() {

        it('to a Number, it updates calendar', function() {
          $rootScope.date = parseInt((new Date('November 7, 2005 23:30:00')).getTime(), 10);
          $rootScope.$digest();
          testCalendar();
          expect(angular.isNumber($rootScope.date)).toBe(true);
        });

        it('to a string that can be parsed by Date, it updates calendar', function() {
          $rootScope.date = 'November 7, 2005 23:30:00';
          $rootScope.$digest();
          testCalendar();
          expect(angular.isString($rootScope.date)).toBe(true);
        });

        it('to a string that cannot be parsed by Date, it gets invalid', function() {
          $rootScope.date = 'pizza';
          $rootScope.$digest();
          expect(element.hasClass('ng-invalid')).toBeTruthy();
          expect(element.hasClass('ng-invalid-date')).toBeTruthy();
          expect($rootScope.date).toBe('pizza');
        });
      });
    });

    it('does not loop between after max mode', function() {
      expect(getTitle()).toBe('September 2010');

      clickTitleButton();
      expect(getTitle()).toBe('2010');

      clickTitleButton();
      expect(getTitle()).toBe('2001 - 2020');

      clickTitleButton();
      expect(getTitle()).toBe('2001 - 2020');
    });

    describe('month selection mode', function () {
      beforeEach(function() {
        clickTitleButton();
      });

      it('shows the year as title', function() {
        expect(getTitle()).toBe('2010');
      });

      it('shows months as options', function() {
        expect(getOptions()).toEqual([
          ['January', 'February', 'March'],
          ['April', 'May', 'June'],
          ['July', 'August', 'September'],
          ['October', 'November', 'December']
        ]);
      });

      it('does not change the model', function() {
        expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
      });

      it('has `selected` only the correct month', function() {
        expectSelectedElement( 8 );
      });

      it('moves to the previous year when `previous` button is clicked', function() {
        clickPreviousButton();

        expect(getTitle()).toBe('2009');
        expect(getOptions()).toEqual([
          ['January', 'February', 'March'],
          ['April', 'May', 'June'],
          ['July', 'August', 'September'],
          ['October', 'November', 'December']
        ]);

        expectSelectedElement( null );
      });

      it('moves to the next year when `next` button is clicked', function() {
        clickNextButton();

        expect(getTitle()).toBe('2011');
        expect(getOptions()).toEqual([
          ['January', 'February', 'March'],
          ['April', 'May', 'June'],
          ['July', 'August', 'September'],
          ['October', 'November', 'December']
        ]);

        expectSelectedElement( null );
      });

      it('renders correctly when a month is clicked', function() {
        clickPreviousButton(5);
        expect(getTitle()).toBe('2005');

        clickOption( 10 );
        expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
        expect(getTitle()).toBe('November 2005');
        expect(getOptions(true)).toEqual([
          ['30', '31', '01', '02', '03', '04', '05'],
          ['06', '07', '08', '09', '10', '11', '12'],
          ['13', '14', '15', '16', '17', '18', '19'],
          ['20', '21', '22', '23', '24', '25', '26'],
          ['27', '28', '29', '30', '01', '02', '03'],
          ['04', '05', '06', '07', '08', '09', '10']
        ]);

        clickOption( 17 );
        expect($rootScope.date).toEqual(new Date('November 16, 2005 15:30:00'));
      });
    });

    describe('year selection mode', function () {
      beforeEach(function() {
        clickTitleButton();
        clickTitleButton();
      });

      it('shows the year range as title', function() {
        expect(getTitle()).toBe('2001 - 2020');
      });

      it('shows years as options', function() {
        expect(getOptions()).toEqual([
          ['2001', '2002', '2003', '2004', '2005'],
          ['2006', '2007', '2008', '2009', '2010'],
          ['2011', '2012', '2013', '2014', '2015'],
          ['2016', '2017', '2018', '2019', '2020']
        ]);
      });

      it('does not change the model', function() {
        expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
      });

      it('has `selected` only the selected year', function() {
        expectSelectedElement( 9 );
      });

      it('moves to the previous year set when `previous` button is clicked', function() {
        clickPreviousButton();

        expect(getTitle()).toBe('1981 - 2000');
        expect(getOptions()).toEqual([
          ['1981', '1982', '1983', '1984', '1985'],
          ['1986', '1987', '1988', '1989', '1990'],
          ['1991', '1992', '1993', '1994', '1995'],
          ['1996', '1997', '1998', '1999', '2000']
        ]);
        expectSelectedElement( null );
      });

      it('moves to the next year set when `next` button is clicked', function() {
        clickNextButton();

        expect(getTitle()).toBe('2021 - 2040');
        expect(getOptions()).toEqual([
          ['2021', '2022', '2023', '2024', '2025'],
          ['2026', '2027', '2028', '2029', '2030'],
          ['2031', '2032', '2033', '2034', '2035'],
          ['2036', '2037', '2038', '2039', '2040']
        ]);

        expectSelectedElement( null );
      });
    });

    describe('keyboard navigation', function() {
      function getActiveLabel() {
        return element.find('.active').eq(0).text();
      }

      describe('day mode', function() {
        it('will be able to activate previous day', function() {
          triggerKeyDown(element, 'left');
          expect(getActiveLabel()).toBe('29');
        });

        it('will be able to select with enter', function() {
          triggerKeyDown(element, 'left');
          triggerKeyDown(element, 'enter');
          expect($rootScope.date).toEqual(new Date('September 29, 2010 15:30:00'));
        });

        it('will be able to select with space', function() {
          triggerKeyDown(element, 'left');
          triggerKeyDown(element, 'space');
          expect($rootScope.date).toEqual(new Date('September 29, 2010 15:30:00'));
        });

        it('will be able to activate next day', function() {
          triggerKeyDown(element, 'right');
          expect(getActiveLabel()).toBe('01');
          expect(getTitle()).toBe('October 2010');
        });

        it('will be able to activate same day in previous week', function() {
          triggerKeyDown(element, 'up');
          expect(getActiveLabel()).toBe('23');
        });

        it('will be able to activate same day in next week', function() {
          triggerKeyDown(element, 'down');
          expect(getActiveLabel()).toBe('07');
          expect(getTitle()).toBe('October 2010');
        });

        it('will be able to activate same date in previous month', function() {
          triggerKeyDown(element, 'pageup');
          expect(getActiveLabel()).toBe('30');
          expect(getTitle()).toBe('August 2010');
        });

        it('will be able to activate same date in next month', function() {
          triggerKeyDown(element, 'pagedown');
          expect(getActiveLabel()).toBe('30');
          expect(getTitle()).toBe('October 2010');
        });

        it('will be able to activate first day of the month', function() {
          triggerKeyDown(element, 'home');
          expect(getActiveLabel()).toBe('01');
          expect(getTitle()).toBe('September 2010');
        });

        it('will be able to activate last day of the month', function() {
          $rootScope.date = new Date('September 1, 2010 15:30:00');
          $rootScope.$digest();

          triggerKeyDown(element, 'end');
          expect(getActiveLabel()).toBe('30');
          expect(getTitle()).toBe('September 2010');
        });

        it('will be able to move to month mode', function() {
          triggerKeyDown(element, 'up', true);
          expect(getActiveLabel()).toBe('September');
          expect(getTitle()).toBe('2010');
        });

        it('will not respond when trying to move to lower mode', function() {
          triggerKeyDown(element, 'down', true);
          expect(getActiveLabel()).toBe('30');
          expect(getTitle()).toBe('September 2010');
        });
      });

      describe('month mode', function() {
        beforeEach(function() {
          triggerKeyDown(element, 'up', true);
        });

        it('will be able to activate previous month', function() {
          triggerKeyDown(element, 'left');
          expect(getActiveLabel()).toBe('August');
        });

        it('will be able to activate next month', function() {
          triggerKeyDown(element, 'right');
          expect(getActiveLabel()).toBe('October');
        });

        it('will be able to activate same month in previous row', function() {
          triggerKeyDown(element, 'up');
          expect(getActiveLabel()).toBe('June');
        });

        it('will be able to activate same month in next row', function() {
          triggerKeyDown(element, 'down');
          expect(getActiveLabel()).toBe('December');
        });

        it('will be able to activate same date in previous year', function() {
          triggerKeyDown(element, 'pageup');
          expect(getActiveLabel()).toBe('September');
          expect(getTitle()).toBe('2009');
        });

        it('will be able to activate same date in next year', function() {
          triggerKeyDown(element, 'pagedown');
          expect(getActiveLabel()).toBe('September');
          expect(getTitle()).toBe('2011');
        });

        it('will be able to activate first month of the year', function() {
          triggerKeyDown(element, 'home');
          expect(getActiveLabel()).toBe('January');
          expect(getTitle()).toBe('2010');
        });

        it('will be able to activate last month of the year', function() {
          triggerKeyDown(element, 'end');
          expect(getActiveLabel()).toBe('December');
          expect(getTitle()).toBe('2010');
        });

        it('will be able to move to year mode', function() {
          triggerKeyDown(element, 'up', true);
          expect(getActiveLabel()).toBe('2010');
          expect(getTitle()).toBe('2001 - 2020');
        });

        it('will be able to move to day mode', function() {
          triggerKeyDown(element, 'down', true);
          expect(getActiveLabel()).toBe('30');
          expect(getTitle()).toBe('September 2010');
        });

        it('will move to day mode when selecting', function() {
          triggerKeyDown(element, 'left', true);
          triggerKeyDown(element, 'enter', true);
          expect(getActiveLabel()).toBe('30');
          expect(getTitle()).toBe('August 2010');
          expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
        });
      });

      describe('year mode', function() {
        beforeEach(function() {
          triggerKeyDown(element, 'up', true);
          triggerKeyDown(element, 'up', true);
        });

        it('will be able to activate previous year', function() {
          triggerKeyDown(element, 'left');
          expect(getActiveLabel()).toBe('2009');
        });

        it('will be able to activate next year', function() {
          triggerKeyDown(element, 'right');
          expect(getActiveLabel()).toBe('2011');
        });

        it('will be able to activate same year in previous row', function() {
          triggerKeyDown(element, 'up');
          expect(getActiveLabel()).toBe('2005');
        });

        it('will be able to activate same year in next row', function() {
          triggerKeyDown(element, 'down');
          expect(getActiveLabel()).toBe('2015');
        });

        it('will be able to activate same date in previous view', function() {
          triggerKeyDown(element, 'pageup');
          expect(getActiveLabel()).toBe('1990');
        });

        it('will be able to activate same date in next view', function() {
          triggerKeyDown(element, 'pagedown');
          expect(getActiveLabel()).toBe('2030');
        });

        it('will be able to activate first year of the year', function() {
          triggerKeyDown(element, 'home');
          expect(getActiveLabel()).toBe('2001');
        });

        it('will be able to activate last year of the year', function() {
          triggerKeyDown(element, 'end');
          expect(getActiveLabel()).toBe('2020');
        });

        it('will not respond when trying to move to upper mode', function() {
          triggerKeyDown(element, 'up', true);
          expect(getTitle()).toBe('2001 - 2020');
        });

        it('will be able to move to month mode', function() {
          triggerKeyDown(element, 'down', true);
          expect(getActiveLabel()).toBe('September');
          expect(getTitle()).toBe('2010');
        });

        it('will move to month mode when selecting', function() {
          triggerKeyDown(element, 'left', true);
          triggerKeyDown(element, 'enter', true);
          expect(getActiveLabel()).toBe('September');
          expect(getTitle()).toBe('2009');
          expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
        });
      });

      describe('`aria-activedescendant`', function() {
        function checkActivedescendant() {
          var activeId = element.find('table').attr('aria-activedescendant');
          expect(element.find('#' + activeId + ' > button')).toHaveClass('active');
        }

        it('updates correctly', function() {
          triggerKeyDown(element, 'left');
          checkActivedescendant();

          triggerKeyDown(element, 'down');
          checkActivedescendant();

          triggerKeyDown(element, 'up', true);
          checkActivedescendant();

          triggerKeyDown(element, 'up', true);
          checkActivedescendant();
        });
      });

    });

  });

  describe('attribute `starting-day`', function () {
    beforeEach(function() {
      $rootScope.startingDay = 1;
      element = $compile('<datepicker ng-model="date" starting-day="startingDay"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('shows the day labels rotated', function() {
      expect(getLabels()).toEqual(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']);
    });

    it('renders the calendar days correctly', function() {
      expect(getOptions(true)).toEqual([
        ['30', '31', '01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10', '11', '12'],
        ['13', '14', '15', '16', '17', '18', '19'],
        ['20', '21', '22', '23', '24', '25', '26'],
        ['27', '28', '29', '30', '01', '02', '03'],
        ['04', '05', '06', '07', '08', '09', '10']
      ]);
    });

    it('renders the week numbers correctly', function() {
      expect(getWeeks()).toEqual(['35', '36', '37', '38', '39', '40']);
    });
  });

  describe('attribute `show-weeks`', function () {
    var weekHeader, weekElement;
    beforeEach(function() {
      $rootScope.showWeeks = false;
      element = $compile('<datepicker ng-model="date" show-weeks="showWeeks"></datepicker>')($rootScope);
      $rootScope.$digest();

      weekHeader = getLabelsRow().find('th').eq(0);
      weekElement = element.find('tbody').find('tr').eq(1).find('td').eq(0);
    });

    it('hides week numbers based on variable', function() {
      expect(weekHeader.text()).toEqual('');
      expect(weekHeader).toBeHidden();
      expect(weekElement).toBeHidden();
    });
  });

  describe('`min-date` attribute', function () {
    beforeEach(function() {
      $rootScope.mindate = new Date('September 12, 2010');
      element = $compile('<datepicker ng-model="date" min-date="mindate"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('disables appropriate days in current month', function() {
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( index < 14 );
      });
    });

    it('disables appropriate days when min date changes', function() {
      $rootScope.mindate = new Date('September 5, 2010');
      $rootScope.$digest();

      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( index < 7 );
      });
    });

    it('invalidates when model is a disabled date', function() {
      $rootScope.mindate = new Date('September 5, 2010');
      $rootScope.date = new Date('September 2, 2010');
      $rootScope.$digest();
      expect(element.hasClass('ng-invalid')).toBeTruthy();
      expect(element.hasClass('ng-invalid-date-disabled')).toBeTruthy();
    });

    it('disables all days in previous month', function() {
      clickPreviousButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( true );
      });
    });

    it('disables no days in next month', function() {
      clickNextButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( false );
      });
    });

    it('disables appropriate months in current year', function() {
      clickTitleButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( index < 8 );
      });
    });

    it('disables all months in previous year', function() {
      clickTitleButton();
      clickPreviousButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( true );
      });
    });

    it('disables no months in next year', function() {
      clickTitleButton();
      clickNextButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( false );
      });
    });

    it('enables everything before if it is cleared', function() {
      $rootScope.mindate = null;
      $rootScope.date = new Date('December 20, 1949');
      $rootScope.$digest();

      clickTitleButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( false );
      });
    });

  });

  describe('`max-date` attribute', function () {
    beforeEach(function() {
      $rootScope.maxdate = new Date('September 25, 2010');
      element = $compile('<datepicker ng-model="date" max-date="maxdate"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('disables appropriate days in current month', function() {
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( index > 27 );
      });
    });

    it('disables appropriate days when max date changes', function() {
      $rootScope.maxdate = new Date('September 18, 2010');
      $rootScope.$digest();

      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( index > 20 );
      });
    });

    it('invalidates when model is a disabled date', function() {
      $rootScope.maxdate = new Date('September 18, 2010');
      $rootScope.$digest();
      expect(element.hasClass('ng-invalid')).toBeTruthy();
      expect(element.hasClass('ng-invalid-date-disabled')).toBeTruthy();
    });

    it('disables no days in previous month', function() {
      clickPreviousButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( false );
      });
    });

    it('disables all days in next month', function() {
      clickNextButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( true );
      });
    });

    it('disables appropriate months in current year', function() {
      clickTitleButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( index > 8 );
      });
    });

    it('disables no months in previous year', function() {
      clickTitleButton();
      clickPreviousButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( false );
      });
    });

    it('disables all months in next year', function() {
      clickTitleButton();
      clickNextButton();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( true );
      });
    });

    it('enables everything after if it is cleared', function() {
      $rootScope.maxdate = null;
      $rootScope.$digest();
      var buttons = getAllOptionsEl();
      angular.forEach(buttons, function( button, index ) {
        expect(angular.element(button).prop('disabled')).toBe( false );
      });
    });
  });

  describe('date-disabled expression', function () {
    beforeEach(function() {
      $rootScope.dateDisabledHandler = jasmine.createSpy('dateDisabledHandler');
      element = $compile('<datepicker ng-model="date" date-disabled="dateDisabledHandler(date, mode)"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('executes the dateDisabled expression for each visible day plus one for validation', function() {
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(42 + 1);
    });

    it('executes the dateDisabled expression for each visible month plus one for validation', function() {
      $rootScope.dateDisabledHandler.reset();
      clickTitleButton();
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(12 + 1);
    });

    it('executes the dateDisabled expression for each visible year plus one for validation', function() {
      clickTitleButton();
      $rootScope.dateDisabledHandler.reset();
      clickTitleButton();
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(20 + 1);
    });
  });

  describe('formatting', function () {
    beforeEach(function() {
      $rootScope.dayTitle = 'MMMM, yy';
      element = $compile('<datepicker ng-model="date"' +
        'format-day="d"' +
        'format-day-header="EEEE"' +
        'format-day-title="{{dayTitle}}"' +
        'format-month="MMM"' +
        'format-month-title="yy"' +
        'format-year="yy"' +
        'year-range="10"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('changes the title format in `day` mode', function() {
      expect(getTitle()).toBe('September, 10');
    });

    it('changes the title & months format in `month` mode', function() {
      clickTitleButton();

      expect(getTitle()).toBe('10');
      expect(getOptions()).toEqual([
        ['Jan', 'Feb', 'Mar'],
        ['Apr', 'May', 'Jun'],
        ['Jul', 'Aug', 'Sep'],
        ['Oct', 'Nov', 'Dec']
      ]);
    });

    it('changes the title, year format & range in `year` mode', function() {
      clickTitleButton();
      clickTitleButton();

      expect(getTitle()).toBe('01 - 10');
      expect(getOptions()).toEqual([
        ['01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10']
      ]);
    });

    it('shows day labels', function() {
      expect(getLabels()).toEqual(['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']);
    });

    it('changes the day format', function() {
      expect(getOptions(true)).toEqual([
        ['29', '30', '31', '1', '2', '3', '4'],
        ['5', '6', '7', '8', '9', '10', '11'],
        ['12', '13', '14', '15', '16', '17', '18'],
        ['19', '20', '21', '22', '23', '24', '25'],
        ['26', '27', '28', '29', '30', '1', '2'],
        ['3', '4', '5', '6', '7', '8', '9']
      ]);
    });
  });

  describe('setting datepickerConfig', function() {
    var originalConfig = {};
    beforeEach(inject(function(datepickerConfig) {
      angular.extend(originalConfig, datepickerConfig);
      datepickerConfig.formatDay = 'd';
      datepickerConfig.formatMonth = 'MMM';
      datepickerConfig.formatYear = 'yy';
      datepickerConfig.formatDayHeader = 'EEEE';
      datepickerConfig.formatDayTitle = 'MMM, yy';
      datepickerConfig.formatMonthTitle = 'yy';
      datepickerConfig.showWeeks = false;
      datepickerConfig.yearRange = 10;
      datepickerConfig.startingDay = 6;

      element = $compile('<datepicker ng-model="date"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));
    afterEach(inject(function(datepickerConfig) {
      // return it to the original state
      angular.extend(datepickerConfig, originalConfig);
    }));

    it('changes the title format in `day` mode', function() {
      expect(getTitle()).toBe('Sep, 10');
    });

    it('changes the title & months format in `month` mode', function() {
      clickTitleButton();

      expect(getTitle()).toBe('10');
      expect(getOptions()).toEqual([
        ['Jan', 'Feb', 'Mar'],
        ['Apr', 'May', 'Jun'],
        ['Jul', 'Aug', 'Sep'],
        ['Oct', 'Nov', 'Dec']
      ]);
    });

    it('changes the title, year format & range in `year` mode', function() {
      clickTitleButton();
      clickTitleButton();

      expect(getTitle()).toBe('01 - 10');
      expect(getOptions()).toEqual([
        ['01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10']
      ]);
    });

    it('changes the `starting-day` & day headers & format', function() {
      expect(getLabels()).toEqual(['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']);
      expect(getOptions(true)).toEqual([
        ['28', '29', '30', '31', '1', '2', '3'],
        ['4', '5', '6', '7', '8', '9', '10'],
        ['11', '12', '13', '14', '15', '16', '17'],
        ['18', '19', '20', '21', '22', '23', '24'],
        ['25', '26', '27', '28', '29', '30', '1'],
        ['2', '3', '4', '5', '6', '7', '8']
      ]);
    });

    it('changes initial visibility for weeks', function() {
      expect(getLabelsRow().find('th').eq(0)).toBeHidden();
      var tr = element.find('tbody').find('tr');
      for (var i = 0; i < 5; i++) {
        expect(tr.eq(i).find('td').eq(0)).toBeHidden();
      }
    });

  });

  describe('setting datepickerPopupConfig', function() {
    var originalConfig = {};
    beforeEach(inject(function(datepickerPopupConfig) {
      angular.extend(originalConfig, datepickerPopupConfig);
      datepickerPopupConfig.datepickerPopup = 'MM-dd-yyyy';

      element = $compile('<input ng-model="date" datepicker-popup>')($rootScope);
      $rootScope.$digest();
    }));
    afterEach(inject(function(datepickerPopupConfig) {
      // return it to the original state
      angular.extend(datepickerPopupConfig, originalConfig);
    }));

    it('changes date format', function() {
      expect(element.val()).toEqual('09-30-2010');
    });

  });

  describe('as popup', function () {
    var inputEl, dropdownEl, $document, $sniffer;

    function assignElements(wrapElement) {
      inputEl = wrapElement.find('input');
      dropdownEl = wrapElement.find('ul');
      element = dropdownEl.find('table');
    }

    function changeInputValueTo(el, value) {
      el.val(value);
      el.trigger($sniffer.hasEvent('input') ? 'input' : 'change');
      $rootScope.$digest();
    }

    describe('initially', function () {
      beforeEach(inject(function(_$document_, _$sniffer_) {
        $document = _$document_;
        $rootScope.isopen = true;
        $rootScope.date = new Date('September 30, 2010 15:30:00');
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('does not to display datepicker initially', function() {
        expect(dropdownEl).toBeHidden();
      });

      it('to display the correct value in input', function() {
        expect(inputEl.val()).toBe('2010-09-30');
      });
    });

    describe('initially opened', function () {
      beforeEach(inject(function(_$document_, _$sniffer_) {
        $document = _$document_;
        $sniffer = _$sniffer_;
        $rootScope.isopen = true;
        $rootScope.date = new Date('September 30, 2010 15:30:00');
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup is-open="isopen"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('datepicker is displayed', function() {
        expect(dropdownEl).not.toBeHidden();
      });

      it('renders the calendar correctly', function() {
        expect(getLabelsRow().css('display')).not.toBe('none');
        expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
        expect(getOptions(true)).toEqual([
          ['29', '30', '31', '01', '02', '03', '04'],
          ['05', '06', '07', '08', '09', '10', '11'],
          ['12', '13', '14', '15', '16', '17', '18'],
          ['19', '20', '21', '22', '23', '24', '25'],
          ['26', '27', '28', '29', '30', '01', '02'],
          ['03', '04', '05', '06', '07', '08', '09']
        ]);
      });

      it('updates the input when a day is clicked', function() {
        clickOption(17);
        expect(inputEl.val()).toBe('2010-09-15');
        expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
      });

      it('should mark the input field dirty when a day is clicked', function() {
        expect(inputEl).toHaveClass('ng-pristine');
        clickOption(17);
        expect(inputEl).toHaveClass('ng-dirty');
      });

      it('updates the input correctly when model changes', function() {
        $rootScope.date = new Date('January 10, 1983 10:00:00');
        $rootScope.$digest();
        expect(inputEl.val()).toBe('1983-01-10');
      });

      it('closes the dropdown when a day is clicked', function() {
        expect(dropdownEl.css('display')).not.toBe('none');

        clickOption(17);
        expect(dropdownEl.css('display')).toBe('none');
      });

      it('updates the model & calendar when input value changes', function() {
        changeInputValueTo(inputEl, 'March 5, 1980');

        expect($rootScope.date.getFullYear()).toEqual(1980);
        expect($rootScope.date.getMonth()).toEqual(2);
        expect($rootScope.date.getDate()).toEqual(5);

        expect(getOptions(true)).toEqual([
          ['24', '25', '26', '27', '28', '29', '01'],
          ['02', '03', '04', '05', '06', '07', '08'],
          ['09', '10', '11', '12', '13', '14', '15'],
          ['16', '17', '18', '19', '20', '21', '22'],
          ['23', '24', '25', '26', '27', '28', '29'],
          ['30', '31', '01', '02', '03', '04', '05']
        ]);
        expectSelectedElement( 10 );
      });

      it('closes when click outside of calendar', function() {
        expect(dropdownEl).not.toBeHidden();

        $document.find('body').click();
        expect(dropdownEl.css('display')).toBe('none');
      });

      it('sets `ng-invalid` for invalid input', function() {
        changeInputValueTo(inputEl, 'pizza');

        expect(inputEl).toHaveClass('ng-invalid');
        expect(inputEl).toHaveClass('ng-invalid-date');
        expect($rootScope.date).toBeUndefined();
        expect(inputEl.val()).toBe('pizza');
      });

      it('unsets `ng-invalid` for valid input', function() {
        changeInputValueTo(inputEl, 'pizza');
        expect(inputEl).toHaveClass('ng-invalid-date');

        $rootScope.date = new Date('August 11, 2013');
        $rootScope.$digest();
        expect(inputEl).not.toHaveClass('ng-invalid');
        expect(inputEl).not.toHaveClass('ng-invalid-date');
      });

      describe('focus', function () {
        beforeEach(function() {
          var body = $document.find('body');
          body.append(inputEl);
          body.append(dropdownEl);
        });

        afterEach(function() {
          inputEl.remove();
          dropdownEl.remove();
        });

        it('returns to the input when ESC key is pressed in the popup and closes', function() {
          expect(dropdownEl).not.toBeHidden();

          dropdownEl.find('button').eq(0).focus();
          expect(document.activeElement.tagName).toBe('BUTTON');

          triggerKeyDown(dropdownEl, 'esc');
          expect(dropdownEl).toBeHidden();
          expect(document.activeElement.tagName).toBe('INPUT');
        });

        it('returns to the input when ESC key is pressed in the input and closes', function() {
          expect(dropdownEl).not.toBeHidden();

          dropdownEl.find('button').eq(0).focus();
          expect(document.activeElement.tagName).toBe('BUTTON');

          triggerKeyDown(inputEl, 'esc');
          $rootScope.$digest();
          expect(dropdownEl).toBeHidden();
          expect(document.activeElement.tagName).toBe('INPUT');
        });
      });
    });

    describe('attribute `datepickerOptions`', function () {
      var weekHeader, weekElement;
      beforeEach(function() {
        $rootScope.opts = {
          'show-weeks': false
        };
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup datepicker-options="opts" is-open="true"></div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);

        weekHeader = getLabelsRow().find('th').eq(0);
        weekElement = element.find('tbody').find('tr').eq(1).find('td').eq(0);
      });

      it('hides week numbers based on variable', function() {
        expect(weekHeader.text()).toEqual('');
        expect(weekHeader).toBeHidden();
        expect(weekElement).toBeHidden();
      });
    });

    describe('toggles programatically by `open` attribute', function () {
      beforeEach(inject(function() {
        $rootScope.open = true;
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup is-open="open"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('to display initially', function() {
        expect(dropdownEl.css('display')).not.toBe('none');
      });

      it('to close / open from scope variable', function() {
        expect(dropdownEl.css('display')).not.toBe('none');
        $rootScope.open = false;
        $rootScope.$digest();
        expect(dropdownEl.css('display')).toBe('none');

        $rootScope.open = true;
        $rootScope.$digest();
        expect(dropdownEl.css('display')).not.toBe('none');
      });
    });

    describe('custom format', function () {
      beforeEach(inject(function() {
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup="dd-MMMM-yyyy"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('to display the correct value in input', function() {
        expect(inputEl.val()).toBe('30-September-2010');
      });

      it('updates the input when a day is clicked', function() {
        clickOption(17);
        expect(inputEl.val()).toBe('15-September-2010');
        expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
      });

      it('updates the input correctly when model changes', function() {
        $rootScope.date = new Date('January 10, 1983 10:00:00');
        $rootScope.$digest();
        expect(inputEl.val()).toBe('10-January-1983');
      });
    });

    describe('dynamic custom format', function () {
      beforeEach(inject(function() {
        $rootScope.format = 'dd-MMMM-yyyy';
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup="{{format}}"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('to display the correct value in input', function() {
        expect(inputEl.val()).toBe('30-September-2010');
      });

      it('updates the input when a day is clicked', function() {
        clickOption(17);
        expect(inputEl.val()).toBe('15-September-2010');
        expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
      });

      it('updates the input correctly when model changes', function() {
        $rootScope.date = new Date('August 11, 2013 09:09:00');
        $rootScope.$digest();
        expect(inputEl.val()).toBe('11-August-2013');
      });

      it('updates the input correctly when format changes', function() {
        $rootScope.format = 'dd/MM/yyyy';
        $rootScope.$digest();
        expect(inputEl.val()).toBe('30/09/2010');
      });
    });

    describe('european format', function () {
      it('dd.MM.yyyy', function() {
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup="dd.MM.yyyy"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);

        changeInputValueTo(inputEl, '11.08.2013');
        expect($rootScope.date.getFullYear()).toEqual(2013);
        expect($rootScope.date.getMonth()).toEqual(7);
        expect($rootScope.date.getDate()).toEqual(11);
      });
    });

    describe('`close-on-date-selection` attribute', function () {
      beforeEach(inject(function() {
        $rootScope.close = false;
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup close-on-date-selection="close" is-open="true"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('does not close the dropdown when a day is clicked', function() {
        clickOption(17);
        expect(dropdownEl.css('display')).not.toBe('none');
      });
    });

    describe('button bar', function() {
      var buttons, buttonBarElement;

      function assignButtonBar() {
        buttonBarElement = dropdownEl.find('li').eq(-1);
        buttons = buttonBarElement.find('button');
      }

      describe('', function () {
        beforeEach(inject(function() {
          $rootScope.isopen = true;
          var wrapElement = $compile('<div><input ng-model="date" datepicker-popup is-open="isopen"><div>')($rootScope);
          $rootScope.$digest();
          assignElements(wrapElement);
          assignButtonBar();
        }));

        it('should exist', function() {
          expect(dropdownEl).not.toBeHidden();
          expect(dropdownEl.find('li').length).toBe(2);
        });

        it('should have three buttons', function() {
          expect(buttons.length).toBe(3);

          expect(buttons.eq(0).text()).toBe('Today');
          expect(buttons.eq(1).text()).toBe('Clear');
          expect(buttons.eq(2).text()).toBe('Done');
        });

        it('should have a button to set today date without altering time part', function() {
          var today = new Date();
          buttons.eq(0).click();
          expect($rootScope.date.getFullYear()).toBe(today.getFullYear());
          expect($rootScope.date.getMonth()).toBe(today.getMonth());
          expect($rootScope.date.getDate()).toBe(today.getDate());

          expect($rootScope.date.getHours()).toBe(15);
          expect($rootScope.date.getMinutes()).toBe(30);
          expect($rootScope.date.getSeconds()).toBe(0);
        });

        it('should have a button to set today date if blank', function() {
          $rootScope.date = null;
          $rootScope.$digest();

          var today = new Date();
          buttons.eq(0).click();
          expect($rootScope.date.getFullYear()).toBe(today.getFullYear());
          expect($rootScope.date.getMonth()).toBe(today.getMonth());
          expect($rootScope.date.getDate()).toBe(today.getDate());

          expect($rootScope.date.getHours()).toBe(0);
          expect($rootScope.date.getMinutes()).toBe(0);
          expect($rootScope.date.getSeconds()).toBe(0);
        });

        it('should have a button to clear value', function() {
          buttons.eq(1).click();
          expect($rootScope.date).toBe(null);
        });

        it('should have a button to close calendar', function() {
          buttons.eq(2).click();
          expect(dropdownEl).toBeHidden();
        });
      });

      describe('customization', function() {
        it('should change text from attributes', function() {
          $rootScope.clearText = 'Null it!';
          $rootScope.close = 'Close';
          var wrapElement = $compile('<div><input ng-model="date" datepicker-popup current-text="Now" clear-text="{{clearText}}" close-text="{{close}}ME"><div>')($rootScope);
          $rootScope.$digest();
          assignElements(wrapElement);
          assignButtonBar();

          expect(buttons.eq(0).text()).toBe('Now');
          expect(buttons.eq(1).text()).toBe('Null it!');
          expect(buttons.eq(2).text()).toBe('CloseME');
        });

        it('should remove bar', function() {
          $rootScope.showBar = false;
          var wrapElement = $compile('<div><input ng-model="date" datepicker-popup show-button-bar="showBar"><div>')($rootScope);
          $rootScope.$digest();
          assignElements(wrapElement);
          expect(dropdownEl.find('li').length).toBe(1);
        });
      });

      describe('`ng-change`', function() {
        beforeEach(inject(function() {
          $rootScope.changeHandler = jasmine.createSpy('changeHandler');
          var wrapElement = $compile('<div><input ng-model="date" datepicker-popup ng-change="changeHandler()"><div>')($rootScope);
          $rootScope.$digest();
          assignElements(wrapElement);
          assignButtonBar();
        }));

        it('should be called when `today` is clicked', function() {
          buttons.eq(0).click();
          expect($rootScope.changeHandler).toHaveBeenCalled();
        });

        it('should be called when `clear` is clicked', function() {
          buttons.eq(1).click();
          expect($rootScope.changeHandler).toHaveBeenCalled();
        });

        it('should not be called when `close` is clicked', function() {
          buttons.eq(2).click();
          expect($rootScope.changeHandler).not.toHaveBeenCalled();
        });
      });
    });

    describe('use with `ng-required` directive', function() {
      beforeEach(inject(function() {
        $rootScope.date = '';
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup ng-required="true"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('should be invalid initially', function() {
        expect(inputEl.hasClass('ng-invalid')).toBeTruthy();
      });
      it('should be valid if model has been specified', function() {
        $rootScope.date = new Date();
        $rootScope.$digest();
        expect(inputEl.hasClass('ng-valid')).toBeTruthy();
      });
    });

    describe('use with `ng-change` directive', function() {
      beforeEach(inject(function() {
        $rootScope.changeHandler = jasmine.createSpy('changeHandler');
        $rootScope.date = new Date();
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup ng-required="true" ng-change="changeHandler()"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('should not be called initially', function() {
        expect($rootScope.changeHandler).not.toHaveBeenCalled();
      });

      it('should be called when a day is clicked', function() {
        clickOption(17);
        expect($rootScope.changeHandler).toHaveBeenCalled();
      });

      it('should not be called when model changes programatically', function() {
        $rootScope.date = new Date();
        $rootScope.$digest();
        expect($rootScope.changeHandler).not.toHaveBeenCalled();
      });
    });

    describe('with an append-to-body attribute', function() {
      beforeEach(function() {
        $rootScope.date = new Date();
      });

      afterEach(function () {
        $document.find('body').find('.dropdown-menu').remove();
      });

      it('should append to the body', function() {
        var $body = $document.find('body'),
            bodyLength = $body.children().length,
            elm = angular.element(
              '<div><input datepicker-popup ng-model="date" datepicker-append-to-body="true"></input></div>'
            );
        $compile(elm)($rootScope);
        $rootScope.$digest();

        expect($body.children().length).toEqual(bodyLength + 1);
        expect(elm.children().length).toEqual(1);
      });
      it('should be removed on scope destroy', function() {
        var $body = $document.find('body'),
            bodyLength = $body.children().length,
            isolatedScope = $rootScope.$new(),
            elm = angular.element(
              '<input datepicker-popup ng-model="date" datepicker-append-to-body="true"></input>'
            );
        $compile(elm)(isolatedScope);
        isolatedScope.$digest();
        expect($body.children().length).toEqual(bodyLength + 1);
        isolatedScope.$destroy();
        expect($body.children().length).toEqual(bodyLength);
      });
    });

    describe('with setting datepickerConfig.showWeeks to false', function() {
      var originalConfig = {};
      beforeEach(inject(function(datepickerConfig) {
        angular.extend(originalConfig, datepickerConfig);
        datepickerConfig.showWeeks = false;

        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));
      afterEach(inject(function(datepickerConfig) {
        // return it to the original state
        angular.extend(datepickerConfig, originalConfig);
      }));

      it('changes initial visibility for weeks', function() {
        expect(getLabelsRow().find('th').eq(0)).toBeHidden();
        var tr = element.find('tbody').find('tr');
        for (var i = 0; i < 5; i++) {
          expect(tr.eq(i).find('td').eq(0)).toBeHidden();
        }
      });
    });

    describe('`datepicker-mode`', function () {
      beforeEach(inject(function() {
        $rootScope.date = new Date('August 11, 2013');
        $rootScope.mode = 'month';
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup datepicker-mode="mode"></div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('shows the correct title', function() {
        expect(getTitle()).toBe('2013');
      });

      it('updates binding', function() {
        clickTitleButton();
        expect($rootScope.mode).toBe('year');
      });
    });
  });

  describe('with empty initial state', function () {
    beforeEach(inject(function() {
      $rootScope.date = null;
      element = $compile('<datepicker ng-model="date"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));

    it('is has a `<table>` element', function() {
      expect(element.find('table').length).toBe(1);
    });

    it('is shows rows with days', function() {
      expect(element.find('tbody').find('tr').length).toBeGreaterThan(3);
    });

    it('sets default 00:00:00 time for selected date', function() {
      $rootScope.date = new Date('August 1, 2013');
      $rootScope.$digest();
      $rootScope.date = null;
      $rootScope.$digest();

      clickOption(14);
      expect($rootScope.date).toEqual(new Date('August 11, 2013 00:00:00'));
    });
  });

  describe('`init-date`', function () {
    beforeEach(inject(function() {
      $rootScope.date = null;
      $rootScope.initDate = new Date('November 9, 1980');
      element = $compile('<datepicker ng-model="date" init-date="initDate"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));

    it('does not alter the model', function() {
      expect($rootScope.date).toBe(null);
    });

    it('shows the correct title', function() {
      expect(getTitle()).toBe('November 1980');
    });
  });

  describe('`datepicker-mode`', function () {
    beforeEach(inject(function() {
      $rootScope.date = new Date('August 11, 2013');
      $rootScope.mode = 'month';
      element = $compile('<datepicker ng-model="date" datepicker-mode="mode"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));

    it('shows the correct title', function() {
      expect(getTitle()).toBe('2013');
    });

    it('updates binding', function() {
      clickTitleButton();
      expect($rootScope.mode).toBe('year');
    });
  });

  describe('`min-mode`', function () {
    beforeEach(inject(function() {
      $rootScope.date = new Date('August 11, 2013');
      $rootScope.mode = 'month';
      element = $compile('<datepicker ng-model="date" min-mode="month" datepicker-mode="mode"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));

    it('does not move below it', function() {
      expect(getTitle()).toBe('2013');
      clickOption( 5 );
      expect(getTitle()).toBe('2013');
      clickTitleButton();
      expect(getTitle()).toBe('2001 - 2020');
    });
  });

  describe('`max-mode`', function () {
    beforeEach(inject(function() {
      $rootScope.date = new Date('August 11, 2013');
      element = $compile('<datepicker ng-model="date" max-mode="month"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));

    it('does not move above it', function() {
      expect(getTitle()).toBe('August 2013');
      clickTitleButton();
      expect(getTitle()).toBe('2013');
      clickTitleButton();
      expect(getTitle()).toBe('2013');
    });
  });
});
