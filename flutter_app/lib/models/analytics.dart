class TimelinePoint {
  final String date;
  final int count;

  const TimelinePoint({required this.date, required this.count});

  factory TimelinePoint.fromJson(Map<String, dynamic> json) {
    return TimelinePoint(
      date: json['date'] as String,
      count: json['count'] as int,
    );
  }

  DateTime get dateTime => DateTime.parse(date);
}

class TopCompany {
  final String company;
  final int count;

  const TopCompany({required this.company, required this.count});

  factory TopCompany.fromJson(Map<String, dynamic> json) {
    return TopCompany(
      company: json['company'] as String,
      count: (json['count'] as num).toInt(),
    );
  }
}

class AnalyticsBucket {
  final String label;
  final int count;

  const AnalyticsBucket({required this.label, required this.count});

  factory AnalyticsBucket.fromJson(Map<String, dynamic> json) {
    return AnalyticsBucket(
      label: json['label'] as String,
      count: (json['count'] as num).toInt(),
    );
  }
}

class AnalyticsData {
  final int totalOpportunities;
  final int newToday;
  final int deadlinesThisWeek;
  final List<AnalyticsBucket> deadlineHealth;
  final List<AnalyticsBucket> eligibilityBreakdown;
  final List<AnalyticsBucket> locationDistribution;
  final List<AnalyticsBucket> packageBands;
  final List<TopCompany> topCompanies;
  final List<TimelinePoint> timeline;

  const AnalyticsData({
    required this.totalOpportunities,
    required this.newToday,
    required this.deadlinesThisWeek,
    required this.deadlineHealth,
    required this.eligibilityBreakdown,
    required this.locationDistribution,
    required this.packageBands,
    required this.topCompanies,
    required this.timeline,
  });
}