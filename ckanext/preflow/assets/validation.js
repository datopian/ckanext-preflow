

ckan.module("validation-report", function ($, _) {
  return {
    options: {
      report: {},
    },

    initialize: function () {
      const element = this.el[0]
      const reportJson = this.options.report
      frictionlessComponents.render(
        frictionlessComponents.Report,
        { report: reportJson },
        element
      );
    },
  };
});
