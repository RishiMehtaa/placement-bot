import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'package:url_launcher/url_launcher.dart';
import '../core/theme.dart';
import '../providers/opportunities_provider.dart';
import '../widgets/stat_card.dart';
import '../widgets/opportunity_card.dart';
import '../widgets/opportunity_table.dart';

class DashboardScreen extends ConsumerStatefulWidget {
  const DashboardScreen({super.key});

  @override
  ConsumerState<DashboardScreen> createState() => _DashboardScreenState();
}

class _DashboardScreenState extends ConsumerState<DashboardScreen> {
  final TextEditingController _searchController = TextEditingController();

  Future<void> _openIntegrationLink(String linkKey, String label) async {
    try {
      final links = await ref.read(integrationLinksProvider.future);
      final targetUrl = links[linkKey] ?? '';

      if (targetUrl.isEmpty) {
        if (!mounted) return;
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('$label link is not configured yet.')),
        );
        return;
      }

      final uri = Uri.tryParse(targetUrl);
      if (uri != null && await canLaunchUrl(uri)) {
        await launchUrl(uri, mode: LaunchMode.externalApplication);
        return;
      }

      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Could not open $label link.')),
      );
    } catch (_) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Failed to load $label link.')),
      );
    }
  }

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final size = MediaQuery.of(context).size;
    final isDesktop = size.width >= 768;

    final opportunitiesAsync = ref.watch(opportunitiesProvider);
    final summaryAsync = ref.watch(analyticsSummaryProvider);

    return Scaffold(
      appBar: AppBar(
        title: const Text('Placement Intelligence'),
        automaticallyImplyLeading: false,
        actions: [
          IconButton(
            icon: const Icon(Icons.bar_chart_rounded),
            tooltip: 'Analytics',
            onPressed: () => context.go('/analytics'),
          ),
          IconButton(
            icon: const Icon(Icons.table_chart_rounded),
            tooltip: 'Open Google Sheet',
            onPressed: () => _openIntegrationLink('google_sheet_url', 'Google Sheet'),
          ),
          IconButton(
            icon: const Icon(Icons.calendar_month_rounded),
            tooltip: 'Open Google Calendar',
            onPressed: () => _openIntegrationLink('google_calendar_url', 'Google Calendar'),
          ),
          IconButton(
            icon: const Icon(Icons.file_upload_rounded),
            tooltip: 'Import Chat Export',
            onPressed: () => context.go('/import'),
          ),
          IconButton(
            icon: const Icon(Icons.refresh),
            tooltip: 'Refresh',
            onPressed: () {
              ref.invalidate(opportunitiesProvider);
              ref.invalidate(analyticsSummaryProvider);
              ref.invalidate(integrationLinksProvider);
            },
          ),
          const SizedBox(width: 8),
        ],
      ),
      body: RefreshIndicator(
        color: AppTheme.primary,
        onRefresh: () async {
          ref.invalidate(opportunitiesProvider);
          ref.invalidate(analyticsSummaryProvider);
          ref.invalidate(integrationLinksProvider);
        },
        child: CustomScrollView(
          slivers: [
            SliverPadding(
              padding: EdgeInsets.all(isDesktop ? 32 : 16),
              sliver: SliverList(
                delegate: SliverChildListDelegate([
                  // Header
                  const Row(
                    children: [
                       Expanded(
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              'Opportunities',
                              style: TextStyle(
                                fontSize: 22,
                                fontWeight: FontWeight.w700,
                                color: AppTheme.textPrimary,
                              ),
                            ),
                            SizedBox(height: 4),
                            Text(
                              'All placement opportunities from your group',
                              style: TextStyle(
                                fontSize: 13,
                                color: AppTheme.textSecondary,
                              ),
                            ),
                          ],
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 24),

                  // Stat cards
                  summaryAsync.when(
                    loading: () => _StatCardsShimmer(isDesktop: isDesktop),
                    error: (e, _) => _ErrorBanner(message: e.toString()),
                    data: (summary) => GridView.count(
                      crossAxisCount: isDesktop ? 3 : 2,
                      crossAxisSpacing: 12,
                      mainAxisSpacing: 12,
                      shrinkWrap: true,
                      physics: const NeverScrollableScrollPhysics(),
                      childAspectRatio: isDesktop ? 2.2 : 1.6,
                      children: [
                        StatCard(
                          label: 'Total Opportunities',
                          value: summary.totalOpportunities.toString(),
                          icon: Icons.work_outline_rounded,
                          color: AppTheme.primary,
                        ),
                        StatCard(
                          label: 'New Today',
                          value: summary.newToday.toString(),
                          icon: Icons.today_rounded,
                          color: AppTheme.accent,
                        ),
                        StatCard(
                          label: 'Deadlines This Week',
                          value: summary.deadlinesThisWeek.toString(),
                          icon: Icons.timer_outlined,
                          color: AppTheme.warning,
                        ),
                      ],
                    ),
                  ),
                  const SizedBox(height: 24),

                  // Search bar with sort
                  Row(
                    children: [
                      Expanded(
                        child: TextField(
                          controller: _searchController,
                          style: const TextStyle(color: AppTheme.textPrimary),
                          decoration: InputDecoration(
                            hintText: 'Search by company or role...',
                            prefixIcon: const Icon(Icons.search, color: AppTheme.textSecondary, size: 20),
                            suffixIcon: _searchController.text.isNotEmpty
                                ? IconButton(
                                    icon: const Icon(Icons.clear, color: AppTheme.textSecondary, size: 18),
                                    onPressed: () {
                                      _searchController.clear();
                                      ref.read(searchQueryProvider.notifier).state = '';
                                      ref.read(currentPageProvider.notifier).state = 1;
                                    },
                                  )
                                : null,
                          ),
                          onChanged: (val) {
                            ref.read(searchQueryProvider.notifier).state = val;
                            ref.read(currentPageProvider.notifier).state = 1;
                          },
                        ),
                      ),
                      const SizedBox(width: 12),
                      // Sort dropdown
                      Container(
                        padding: const EdgeInsets.symmetric(horizontal: 12),
                        decoration: BoxDecoration(
                          borderRadius: BorderRadius.circular(8),
                          border: Border.all(color: AppTheme.border),
                        ),
                        child: DropdownButton<String>(
                          value: ref.watch(sortByProvider),
                          items: const [
                            DropdownMenuItem(value: 'deadline', child: Text('Deadline')),
                            DropdownMenuItem(value: 'package', child: Text('Package')),
                            DropdownMenuItem(value: 'created_at', child: Text('Newest')),
                            DropdownMenuItem(value: 'confidence', child: Text('Confidence')),
                          ],
                          onChanged: (value) {
                            if (value != null) {
                              ref.read(sortByProvider.notifier).state = value;
                              ref.read(currentPageProvider.notifier).state = 1;
                            }
                          },
                          underline: const SizedBox(),
                          isDense: true,
                          style: const TextStyle(color: AppTheme.textPrimary, fontSize: 13),
                          dropdownColor: AppTheme.surface,
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 16),

                  // Filter chips
                  SingleChildScrollView(
                    scrollDirection: Axis.horizontal,
                    child: Row(
                      children: [
                        // Package range filter
                        PopupMenuButton<String?>(
                          initialValue: ref.watch(packageFilterProvider),
                          itemBuilder: (context) => [
                            const PopupMenuItem(value: null, child: Text('All Packages')),
                            const PopupMenuItem(value: 'below_3', child: Text('< 3 LPA')),
                            const PopupMenuItem(value: '3_to_6', child: Text('3-6 LPA')),
                            const PopupMenuItem(value: '6_to_10', child: Text('6-10 LPA')),
                            const PopupMenuItem(value: 'above_10', child: Text('10+ LPA')),
                          ],
                          onSelected: (value) {
                            ref.read(packageFilterProvider.notifier).state = value;
                            ref.read(currentPageProvider.notifier).state = 1;
                          },
                          child: Chip(
                            label: Text(
                              ref.watch(packageFilterProvider) != null
                                  ? 'Package: ${_formatPackageFilter(ref.watch(packageFilterProvider))}'
                                  : 'Package',
                              style: const TextStyle(fontSize: 12),
                            ),
                            onDeleted: ref.watch(packageFilterProvider) != null
                                ? () {
                                    ref.read(packageFilterProvider.notifier).state = null;
                                    ref.read(currentPageProvider.notifier).state = 1;
                                  }
                                : null,
                            backgroundColor: ref.watch(packageFilterProvider) != null
                                ? AppTheme.primary.withOpacity(0.2)
                                : AppTheme.surface,
                          ),
                        ),
                        const SizedBox(width: 8),
                        // Deadline status filter
                        PopupMenuButton<String?>(
                          initialValue: ref.watch(deadlineStatusFilterProvider),
                          itemBuilder: (context) => [
                            const PopupMenuItem(value: null, child: Text('All Deadlines')),
                            const PopupMenuItem(value: 'overdue', child: Text('Overdue')),
                            const PopupMenuItem(value: 'due_soon', child: Text('Due Soon (≤7 days)')),
                            const PopupMenuItem(value: 'open', child: Text('Open (>7 days)')),
                          ],
                          onSelected: (value) {
                            ref.read(deadlineStatusFilterProvider.notifier).state = value;
                            ref.read(currentPageProvider.notifier).state = 1;
                          },
                          child: Chip(
                            label: Text(
                              ref.watch(deadlineStatusFilterProvider) != null
                                  ? 'Deadline: ${_formatDeadlineFilter(ref.watch(deadlineStatusFilterProvider))}'
                                  : 'Deadline',
                              style: const TextStyle(fontSize: 12),
                            ),
                            onDeleted: ref.watch(deadlineStatusFilterProvider) != null
                                ? () {
                                    ref.read(deadlineStatusFilterProvider.notifier).state = null;
                                    ref.read(currentPageProvider.notifier).state = 1;
                                  }
                                : null,
                            backgroundColor: ref.watch(deadlineStatusFilterProvider) != null
                                ? AppTheme.accent.withOpacity(0.2)
                                : AppTheme.surface,
                          ),
                        ),
                      ],
                    ),
                  ),
                  const SizedBox(height: 20),

                  // Opportunities list
                  opportunitiesAsync.when(
                    loading: () => const Center(
                      child: Padding(
                        padding: EdgeInsets.all(48),
                        child: CircularProgressIndicator(color: AppTheme.primary),
                      ),
                    ),
                    error: (e, _) => _ErrorBanner(message: e.toString()),
                    data: (page) {
                      if (isDesktop) {
                        return OpportunityTable(opportunities: page.opportunities);
                      } else {
                        if (page.opportunities.isEmpty) {
                          return Center(
                            child: Column(
                              children: [
                                const SizedBox(height: 48),
                                Icon(
                                  Icons.inbox_rounded,
                                  size: 48,
                                  color: AppTheme.textSecondary.withOpacity(0.5),
                                ),
                                const SizedBox(height: 16),
                                const Text(
                                  'No opportunities found',
                                  style: TextStyle(color: AppTheme.textSecondary),
                                ),
                              ],
                            ),
                          );
                        }
                        return Column(
                          children: page.opportunities
                              .map((opp) => Padding(
                                    padding: const EdgeInsets.only(bottom: 12),
                                    child: OpportunityCard(opportunity: opp),
                                  ))
                              .toList(),
                        );
                      }
                    },
                  ),

                  // Pagination
                  // opportunitiesAsync.whenData((page) {
                  //   if (page.totalPages <= 1) return const SizedBox.shrink();
                  //   return null;
                  // }),
                  opportunitiesAsync.when(
                    loading: () => const SizedBox.shrink(),
                    error: (_, __) => const SizedBox.shrink(),
                    data: (page) {
                      if (page.totalPages <= 1) return const SizedBox.shrink();
                      final currentPage = ref.watch(currentPageProvider);
                      return Padding(
                        padding: const EdgeInsets.only(top: 24),
                        child: Row(
                          mainAxisAlignment: MainAxisAlignment.center,
                          children: [
                            IconButton(
                              icon: const Icon(Icons.chevron_left, color: AppTheme.textPrimary),
                              onPressed: currentPage > 1
                                  ? () => ref.read(currentPageProvider.notifier).state--
                                  : null,
                            ),
                            Text(
                              'Page $currentPage of ${page.totalPages}',
                              style: const TextStyle(color: AppTheme.textSecondary, fontSize: 13),
                            ),
                            IconButton(
                              icon: const Icon(Icons.chevron_right, color: AppTheme.textPrimary),
                              onPressed: currentPage < page.totalPages
                                  ? () => ref.read(currentPageProvider.notifier).state++
                                  : null,
                            ),
                          ],
                        ),
                      );
                    },
                  ),

                  const SizedBox(height: 48),
                ]),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _StatCardsShimmer extends StatelessWidget {
  final bool isDesktop;
  const _StatCardsShimmer({required this.isDesktop});

  @override
  Widget build(BuildContext context) {
    return GridView.count(
      crossAxisCount: isDesktop ? 3 : 2,
      crossAxisSpacing: 12,
      mainAxisSpacing: 12,
      shrinkWrap: true,
      physics: const NeverScrollableScrollPhysics(),
      childAspectRatio: isDesktop ? 2.2 : 1.6,
      children: List.generate(
        3,
        (_) => Container(
          decoration: BoxDecoration(
            color: AppTheme.surface,
            borderRadius: BorderRadius.circular(12),
            border: Border.all(color: AppTheme.border),
          ),
        ),
      ),
    );
  }
}

class _ErrorBanner extends StatelessWidget {
  final String message;
  const _ErrorBanner({required this.message});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.error.withOpacity(0.1),
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: AppTheme.error.withOpacity(0.3)),
      ),
      child: Row(
        children: [
          const Icon(Icons.error_outline, color: AppTheme.error, size: 18),
          const SizedBox(width: 10),
          Expanded(
            child: Text(
              message,
              style: const TextStyle(color: AppTheme.error, fontSize: 13),
            ),
          ),
        ],
      ),
    );
  }
}

String _formatPackageFilter(String? filter) {
  switch (filter) {
    case 'below_3':
      return '< 3 LPA';
    case '3_to_6':
      return '3-6 LPA';
    case '6_to_10':
      return '6-10 LPA';
    case 'above_10':
      return '10+ LPA';
    default:
      return '';
  }
}

String _formatDeadlineFilter(String? filter) {
  switch (filter) {
    case 'overdue':
      return 'Overdue';
    case 'due_soon':
      return 'Due Soon';
    case 'open':
      return 'Open';
    default:
      return '';
  }
}