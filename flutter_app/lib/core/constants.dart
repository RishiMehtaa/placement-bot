class AppConstants {
  // Change this to your EC2 IP for local testing against live backend
  // Change to CloudFront URL after 18g deployment
  // static const String baseUrl = 'http://65.1.61.251:8000';
static const String baseUrl = 'https://placement-bot.duckdns.org';
  // Optional runtime config via --dart-define values.
  // If these are empty, the app falls back to Google home pages.
  static const String googleSheetId = String.fromEnvironment('GOOGLE_SHEET_ID', defaultValue: '');
  static const String googleCalendarId = String.fromEnvironment('GOOGLE_CALENDAR_ID', defaultValue: 'primary');

  static const String googleSheetsHomeUrl = 'https://docs.google.com/spreadsheets/';
  static const String googleCalendarHomeUrl = 'https://calendar.google.com/calendar/u/0/r';

  static String get googleSheetUrlFallback {
    if (googleSheetId.isEmpty) {
      return googleSheetsHomeUrl;
    }
    return 'https://docs.google.com/spreadsheets/d/$googleSheetId/edit';
  }

  static String get googleCalendarUrlFallback {
    if (googleCalendarId == 'primary') {
      return googleCalendarHomeUrl;
    }
    final encodedCalendarId = Uri.encodeQueryComponent(googleCalendarId);
    return '$googleCalendarHomeUrl?cid=$encodedCalendarId';
  }

  static const String healthEndpoint = '/health';
  static const String opportunitiesEndpoint = '/opportunities';
  static const String analyticsEndpoint = '/analytics/summary';
  static const String timelineEndpoint = '/analytics/timeline';
  static const String chatImportEndpoint = '/chat/import';
  static const String demoQrEndpoint = '/demo/qr';
  static const String demoGroupsEndpoint = '/demo/groups';
}