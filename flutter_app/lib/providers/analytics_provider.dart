import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../models/analytics.dart';
import '../services/api_service.dart';

final analyticsDataProvider = FutureProvider.autoDispose<AnalyticsData>((ref) async {
  final summary = await ApiService().getAnalyticsSummary();
  final timeline = await ApiService().getTimeline();

  return AnalyticsData(
    totalOpportunities: summary.totalOpportunities,
    newToday: summary.newToday,
    deadlinesThisWeek: summary.deadlinesThisWeek,
    topCompanies: summary.topCompanies
        .map((e) => TopCompany.fromJson(e))
        .toList(),
    timeline: timeline
        .map((e) => TimelinePoint.fromJson(e))
        .toList(),
  );
});