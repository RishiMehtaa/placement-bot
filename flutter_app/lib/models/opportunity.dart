class Opportunity {
  final String id;
  final String? company;
  final String? role;
  final String? deadline;
  final String? package;
  final String? jdLink;
  final double? confidence;
  final String? createdAt;
  final String? updatedAt;
  final String? syncStatus;

  const Opportunity({
    required this.id,
    this.company,
    this.role,
    this.deadline,
    this.package,
    this.jdLink,
    this.confidence,
    this.createdAt,
    this.updatedAt,
    this.syncStatus,
  });

  factory Opportunity.fromJson(Map<String, dynamic> json) {
    return Opportunity(
      id: json['id'] as String,
      company: json['company'] as String?,
      role: json['role'] as String?,
      deadline: json['deadline'] as String?,
      package: json['package'] as String?,
      jdLink: json['jd_link'] as String?,
      confidence: (json['confidence'] as num?)?.toDouble(),
      createdAt: json['created_at'] as String?,
      updatedAt: json['updated_at'] as String?,
      syncStatus: json['sync_status'] as String?,
    );
  }

  DateTime? get deadlineDate {
    if (deadline == null) return null;
    try {
      return DateTime.parse(deadline!);
    } catch (_) {
      return null;
    }
  }

  bool get isDeadlineSoon {
    final d = deadlineDate;
    if (d == null) return false;
    return d.difference(DateTime.now()).inDays <= 3 && d.isAfter(DateTime.now());
  }

  bool get isDeadlinePast {
    final d = deadlineDate;
    if (d == null) return false;
    return d.isBefore(DateTime.now());
  }
}

class OpportunitiesPage {
  final List<Opportunity> opportunities;
  final int total;
  final int page;
  final int pageSize;
  final int totalPages;

  const OpportunitiesPage({
    required this.opportunities,
    required this.total,
    required this.page,
    required this.pageSize,
    required this.totalPages,
  });

  factory OpportunitiesPage.fromJson(Map<String, dynamic> json) {
    return OpportunitiesPage(
      opportunities: (json['opportunities'] as List)
          .map((e) => Opportunity.fromJson(e as Map<String, dynamic>))
          .toList(),
      total: json['total'] as int,
      page: json['page'] as int,
      pageSize: json['page_size'] as int,
      totalPages: json['total_pages'] as int,
    );
  }
}

class AnalyticsSummary {
  final int totalOpportunities;
  final int newToday;
  final int deadlinesThisWeek;
  final List<Map<String, dynamic>> deadlineHealth;
  final List<Map<String, dynamic>> eligibilityBreakdown;
  final List<Map<String, dynamic>> locationDistribution;
  final List<Map<String, dynamic>> packageBands;
  final List<Map<String, dynamic>> topCompanies;

  const AnalyticsSummary({
    required this.totalOpportunities,
    required this.newToday,
    required this.deadlinesThisWeek,
    required this.deadlineHealth,
    required this.eligibilityBreakdown,
    required this.locationDistribution,
    required this.packageBands,
    required this.topCompanies,
  });

  factory AnalyticsSummary.fromJson(Map<String, dynamic> json) {
    return AnalyticsSummary(
      totalOpportunities: json['total_opportunities'] as int,
      newToday: json['new_today'] as int,
      deadlinesThisWeek: json['deadlines_this_week'] as int,
      deadlineHealth: (json['deadline_health'] as List?)
              ?.map((e) => e as Map<String, dynamic>)
              .toList() ??
          [],
      eligibilityBreakdown: (json['eligibility_breakdown'] as List?)
              ?.map((e) => e as Map<String, dynamic>)
              .toList() ??
          [],
      locationDistribution: (json['location_distribution'] as List?)
              ?.map((e) => e as Map<String, dynamic>)
              .toList() ??
          [],
      packageBands: (json['package_bands'] as List?)
              ?.map((e) => e as Map<String, dynamic>)
              .toList() ??
          [],
      topCompanies: (json['top_companies'] as List)
          .map((e) => e as Map<String, dynamic>)
          .toList(),
    );
  }
}