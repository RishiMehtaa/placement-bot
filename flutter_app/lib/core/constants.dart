class AppConstants {
  // Change this to your EC2 IP for local testing against live backend
  // Change to CloudFront URL after 18g deployment
  static const String baseUrl = 'http://65.1.61.251:8000';

  static const String healthEndpoint = '/health';
  static const String opportunitiesEndpoint = '/opportunities';
  static const String analyticsEndpoint = '/analytics/summary';
  static const String timelineEndpoint = '/analytics/timeline';
  static const String demoQrEndpoint = '/demo/qr';
  static const String demoGroupsEndpoint = '/demo/groups';
}