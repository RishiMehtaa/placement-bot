import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../models/opportunity.dart';
import '../services/api_service.dart';

// Search query state
final searchQueryProvider = StateProvider<String>((ref) => '');

// Current page state
final currentPageProvider = StateProvider<int>((ref) => 1);

// Opportunities list provider
final opportunitiesProvider = FutureProvider.autoDispose<OpportunitiesPage>((ref) async {
  final search = ref.watch(searchQueryProvider);
  final page = ref.watch(currentPageProvider);
  return ApiService().getOpportunities(
    page: page,
    pageSize: 20,
    search: search.isEmpty ? null : search,
  );
});

// Analytics summary provider
final analyticsSummaryProvider = FutureProvider.autoDispose<AnalyticsSummary>((ref) async {
  return ApiService().getAnalyticsSummary();
});

// Single opportunity provider
final opportunityDetailProvider = FutureProvider.autoDispose.family<Map<String, dynamic>, String>((ref, id) async {
  return ApiService().getOpportunityDetail(id);
});