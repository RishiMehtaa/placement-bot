import 'package:flutter_riverpod/flutter_riverpod.dart';
import '../models/opportunity.dart';
import '../services/api_service.dart';

// Search query state
final searchQueryProvider = StateProvider<String>((ref) => '');

// Current page state
final currentPageProvider = StateProvider<int>((ref) => 1);

// Sort by state (deadline, package, created_at, confidence)
final sortByProvider = StateProvider<String>((ref) => 'deadline');

// Filter states
final packageFilterProvider = StateProvider<String?>((ref) => null);
final deadlineStatusFilterProvider = StateProvider<String?>((ref) => null);

// Raw opportunities provider (from API)
final rawOpportunitiesProvider = FutureProvider.autoDispose<OpportunitiesPage>((ref) async {
  // Fetch all opportunities without pagination for client-side filtering
  return ApiService().getOpportunities(
    page: 1,
    pageSize: 100,
    search: null,
  );
});

// Filtered and sorted opportunities provider
final opportunitiesProvider = FutureProvider.autoDispose<OpportunitiesPage>((ref) async {
  final rawData = await ref.watch(rawOpportunitiesProvider.future);
  final search = ref.watch(searchQueryProvider);
  final sortBy = ref.watch(sortByProvider);
  final packageFilter = ref.watch(packageFilterProvider);
  final deadlineStatusFilter = ref.watch(deadlineStatusFilterProvider);
  final page = ref.watch(currentPageProvider);
  const pageSize = 20;

  var opportunities = rawData.opportunities;

  // Apply search filter
  if (search.isNotEmpty) {
    final query = search.toLowerCase();
    opportunities = opportunities
        .where((opp) =>
            (opp.company?.toLowerCase().contains(query) ?? false) ||
            (opp.role?.toLowerCase().contains(query) ?? false))
        .toList();
  }

  // Apply package range filter
  if (packageFilter != null) {
    opportunities = opportunities.where((opp) {
      if (opp.package == null || opp.package!.isEmpty) return false;
      final match = RegExp(r'[₹\$£€]?\s*(\d+(?:\.\d+)?)').firstMatch(opp.package!);
      if (match == null) return false;
      final value = double.tryParse(match.group(1) ?? '0') ?? 0;

      switch (packageFilter) {
        case 'below_3':
          return value < 3;
        case '3_to_6':
          return value >= 3 && value < 6;
        case '6_to_10':
          return value >= 6 && value < 10;
        case 'above_10':
          return value >= 10;
        default:
          return true;
      }
    }).toList();
  }

  // Apply deadline status filter
  if (deadlineStatusFilter != null) {
    final now = DateTime.now();
    opportunities = opportunities.where((opp) {
      final deadlineDate = opp.deadlineDate;
      if (deadlineDate == null && deadlineStatusFilter != 'open') return false;

      switch (deadlineStatusFilter) {
        case 'overdue':
          return deadlineDate != null && deadlineDate.isBefore(now);
        case 'due_soon':
          return deadlineDate != null &&
              deadlineDate.isAfter(now) &&
              deadlineDate.difference(now).inDays <= 7;
        case 'open':
          return deadlineDate == null || deadlineDate.isAfter(now.add(const Duration(days: 7)));
        default:
          return true;
      }
    }).toList();
  }

  // Apply sorting
  switch (sortBy) {
    case 'package':
      opportunities.sort((a, b) {
        final aVal = _extractPackageValue(a.package);
        final bVal = _extractPackageValue(b.package);
        return bVal.compareTo(aVal); // Descending
      });
      break;
    case 'created_at':
      opportunities.sort((a, b) {
        final aDate = DateTime.tryParse(a.createdAt ?? '') ?? DateTime(1970);
        final bDate = DateTime.tryParse(b.createdAt ?? '') ?? DateTime(1970);
        return bDate.compareTo(aDate); // Newest first
      });
      break;
    case 'confidence':
      opportunities.sort((a, b) {
        final aConf = a.confidence ?? 0;
        final bConf = b.confidence ?? 0;
        return bConf.compareTo(aConf); // Descending
      });
      break;
    case 'deadline':
    default:
      opportunities.sort((a, b) {
        final aPast = a.isDeadlinePast;
        final bPast = b.isDeadlinePast;
        if (aPast && !bPast) return 1;
        if (!aPast && bPast) return -1;
        if (aPast && bPast) {
          // Both past, show most recent deadline first
          return (b.deadlineDate?.compareTo(a.deadlineDate ?? DateTime(1970)) ?? 0);
        }
        // Both future or one/both null
        final aDate = a.deadlineDate ?? DateTime(2099, 12, 31);
        final bDate = b.deadlineDate ?? DateTime(2099, 12, 31);
        return aDate.compareTo(bDate);
      });
  }

  // Apply pagination on filtered results
  final totalFiltered = opportunities.length;
  final totalPages = (totalFiltered + pageSize - 1) ~/ pageSize;
  final startIdx = (page - 1) * pageSize;
  final endIdx = (startIdx + pageSize).clamp(0, opportunities.length);
  final paginatedOpportunities = opportunities.sublist(
    startIdx,
    endIdx.clamp(0, opportunities.length),
  );

  return OpportunitiesPage(
    opportunities: paginatedOpportunities,
    total: totalFiltered,
    page: page,
    pageSize: pageSize,
    totalPages: totalPages,
  );
});

// Helper function to extract package value
double _extractPackageValue(String? package) {
  if (package == null || package.isEmpty) return 0;
  final match = RegExp(r'[₹\$£€]?\s*(\d+(?:\.\d+)?)').firstMatch(package);
  if (match == null) return 0;
  return double.tryParse(match.group(1) ?? '0') ?? 0;
}

// Analytics summary provider
final analyticsSummaryProvider = FutureProvider.autoDispose<AnalyticsSummary>((ref) async {
  return ApiService().getAnalyticsSummary();
});

// Integration links provider (Google Sheet + Google Calendar)
final integrationLinksProvider = FutureProvider.autoDispose<Map<String, String>>((ref) async {
  return ApiService().getIntegrationLinks();
});

// Single opportunity provider
final opportunityDetailProvider = FutureProvider.autoDispose.family<Map<String, dynamic>, String>((ref, id) async {
  return ApiService().getOpportunityDetail(id);
});