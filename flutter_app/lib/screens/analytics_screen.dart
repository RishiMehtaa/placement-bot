import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'package:intl/intl.dart';
import '../core/theme.dart';
import '../models/analytics.dart';
import '../providers/analytics_provider.dart';
import '../widgets/stat_card.dart';

class AnalyticsScreen extends ConsumerWidget {
  const AnalyticsScreen({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final analyticsAsync = ref.watch(analyticsDataProvider);
    final isDesktop = MediaQuery.of(context).size.width >= 768;

    return Scaffold(
      appBar: AppBar(
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () => context.go('/dashboard'),
        ),
        title: const Text('Analytics'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: () => ref.invalidate(analyticsDataProvider),
          ),
          const SizedBox(width: 8),
        ],
      ),
      body: analyticsAsync.when(
        loading: () => const Center(
          child: CircularProgressIndicator(color: AppTheme.primary),
        ),
        error: (e, _) => Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              const Icon(Icons.error_outline, color: AppTheme.error, size: 48),
              const SizedBox(height: 16),
              Text(
                e.toString(),
                style: const TextStyle(color: AppTheme.textSecondary),
                textAlign: TextAlign.center,
              ),
              const SizedBox(height: 24),
              TextButton(
                onPressed: () => ref.invalidate(analyticsDataProvider),
                child: const Text('Retry'),
              ),
            ],
          ),
        ),
        data: (data) => RefreshIndicator(
          color: AppTheme.primary,
          onRefresh: () async => ref.invalidate(analyticsDataProvider),
          child: SingleChildScrollView(
            physics: const AlwaysScrollableScrollPhysics(),
            padding: EdgeInsets.all(isDesktop ? 32 : 16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Page header
                const Text(
                  'Analytics',
                  style: TextStyle(
                    fontSize: 22,
                    fontWeight: FontWeight.w700,
                    color: AppTheme.textPrimary,
                  ),
                ),
                const SizedBox(height: 4),
                const Text(
                  'Insights from your placement group',
                  style: TextStyle(
                    fontSize: 13,
                    color: AppTheme.textSecondary,
                  ),
                ),
                const SizedBox(height: 24),

                // Stat cards
                GridView.count(
                  crossAxisCount: isDesktop ? 4 : 2,
                  crossAxisSpacing: 12,
                  mainAxisSpacing: 12,
                  shrinkWrap: true,
                  physics: const NeverScrollableScrollPhysics(),
                  childAspectRatio: isDesktop ? 2.2 : 1.6,
                  children: [
                    StatCard(
                      label: 'Total Opportunities',
                      value: data.totalOpportunities.toString(),
                      icon: Icons.work_outline_rounded,
                      color: AppTheme.primary,
                    ),
                    StatCard(
                      label: 'New Today',
                      value: data.newToday.toString(),
                      icon: Icons.today_rounded,
                      color: AppTheme.accent,
                    ),
                    StatCard(
                      label: 'Deadlines This Week',
                      value: data.deadlinesThisWeek.toString(),
                      icon: Icons.timer_outlined,
                      color: AppTheme.warning,
                    ),
                    StatCard(
                      label: 'Applied',
                      value: data.appliedCount.toString(),
                      icon: Icons.check_circle_outline,
                      color: AppTheme.primary,
                    ),
                  ],
                ),
                const SizedBox(height: 32),

                if (data.deadlineHealth.isNotEmpty) ...[
                  _SectionHeader(title: 'Deadline Health'),
                  const SizedBox(height: 16),
                  _BucketStatGrid(
                    buckets: data.deadlineHealth,
                    isDesktop: isDesktop,
                    icon: Icons.timer_outlined,
                    color: AppTheme.warning,
                  ),
                  const SizedBox(height: 32),
                ],

                if (data.eligibilityBreakdown.isNotEmpty) ...[
                  _SectionHeader(title: 'Eligibility Breakdown'),
                  const SizedBox(height: 16),
                  _BucketStatGrid(
                    buckets: data.eligibilityBreakdown,
                    isDesktop: isDesktop,
                    icon: Icons.verified_user_outlined,
                    color: AppTheme.accent,
                  ),
                  const SizedBox(height: 32),
                ],

                if (data.locationDistribution.isNotEmpty) ...[
                  _SectionHeader(title: 'Location Distribution'),
                  const SizedBox(height: 16),
                  _BucketBarChart(
                    buckets: data.locationDistribution,
                    barColor: AppTheme.primary,
                  ),
                  const SizedBox(height: 32),
                ],

                if (data.packageBands.isNotEmpty) ...[
                  _SectionHeader(title: 'Package Bands'),
                  const SizedBox(height: 16),
                  _BucketBarChart(
                    buckets: data.packageBands,
                    barColor: AppTheme.accent,
                  ),
                  const SizedBox(height: 32),
                ],

                // Top companies bar chart
                if (data.appliedCompanies.isNotEmpty) ...[
                  _SectionHeader(title: 'Applied Companies'),
                  const SizedBox(height: 16),
                  _TopCompaniesChart(companies: data.appliedCompanies),
                  const SizedBox(height: 32),
                ],

                if (data.topCompanies.isNotEmpty) ...[
                  _SectionHeader(title: 'Top Hiring Companies'),
                  const SizedBox(height: 16),
                  _TopCompaniesChart(companies: data.topCompanies),
                  const SizedBox(height: 32),
                ],

                // Timeline line chart
                if (data.timeline.isNotEmpty) ...[
                  _SectionHeader(title: 'Opportunities Over Time'),
                  const SizedBox(height: 16),
                  _TimelineChart(timeline: data.timeline),
                  const SizedBox(height: 32),
                ],

                // Empty state
                if (data.deadlineHealth.isEmpty &&
                    data.eligibilityBreakdown.isEmpty &&
                    data.locationDistribution.isEmpty &&
                    data.packageBands.isEmpty &&
                  data.appliedCompanies.isEmpty &&
                    data.topCompanies.isEmpty &&
                    data.timeline.isEmpty)
                  Center(
                    child: Column(
                      children: [
                        const SizedBox(height: 48),
                        Icon(
                          Icons.bar_chart_rounded,
                          size: 56,
                          color: AppTheme.textSecondary.withOpacity(0.4),
                        ),
                        const SizedBox(height: 16),
                        const Text(
                          'No data yet.\nOpportunities will appear here once processed.',
                          textAlign: TextAlign.center,
                          style: TextStyle(
                            color: AppTheme.textSecondary,
                            fontSize: 14,
                            height: 1.6,
                          ),
                        ),
                      ],
                    ),
                  ),

                const SizedBox(height: 48),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _SectionHeader extends StatelessWidget {
  final String title;
  const _SectionHeader({required this.title});

  @override
  Widget build(BuildContext context) {
    return Text(
      title,
      style: const TextStyle(
        fontSize: 16,
        fontWeight: FontWeight.w600,
        color: AppTheme.textPrimary,
      ),
    );
  }
}

class _TopCompaniesChart extends StatelessWidget {
  final List<TopCompany> companies;
  const _TopCompaniesChart({required this.companies});

  @override
  Widget build(BuildContext context) {
    final maxCount = companies.map((c) => c.count).reduce((a, b) => a > b ? a : b);

    return Container(
      height: 260,
      padding: const EdgeInsets.fromLTRB(8, 20, 24, 12),
      decoration: BoxDecoration(
        color: AppTheme.surface,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: AppTheme.border),
      ),
      child: BarChart(
        BarChartData(
          alignment: BarChartAlignment.spaceAround,
          maxY: (maxCount + 1).toDouble(),
          barTouchData: BarTouchData(
            touchTooltipData: BarTouchTooltipData(
              getTooltipColor: (_) => AppTheme.surfaceVariant,
              getTooltipItem: (group, groupIndex, rod, rodIndex) {
                return BarTooltipItem(
                  '${companies[groupIndex].company}\n',
                  const TextStyle(
                    color: AppTheme.textPrimary,
                    fontWeight: FontWeight.w600,
                    fontSize: 12,
                  ),
                  children: [
                    TextSpan(
                      text: '${rod.toY.toInt()} role${rod.toY > 1 ? 's' : ''}',
                      style: const TextStyle(
                        color: AppTheme.primary,
                        fontWeight: FontWeight.w500,
                        fontSize: 12,
                      ),
                    ),
                  ],
                );
              },
            ),
          ),
          titlesData: FlTitlesData(
            show: true,
            topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            leftTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 28,
                interval: 1,
                getTitlesWidget: (value, meta) {
                  if (value == value.floorToDouble() && value > 0) {
                    return Text(
                      value.toInt().toString(),
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 11,
                      ),
                    );
                  }
                  return const SizedBox.shrink();
                },
              ),
            ),
            bottomTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 36,
                getTitlesWidget: (value, meta) {
                  final index = value.toInt();
                  if (index < 0 || index >= companies.length) {
                    return const SizedBox.shrink();
                  }
                  final name = companies[index].company;
                  final short = name.length > 10 ? '${name.substring(0, 10)}…' : name;
                  return Padding(
                    padding: const EdgeInsets.only(top: 6),
                    child: Text(
                      short,
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 10,
                      ),
                      textAlign: TextAlign.center,
                    ),
                  );
                },
              ),
            ),
          ),
          gridData: FlGridData(
            show: true,
            drawVerticalLine: false,
            getDrawingHorizontalLine: (_) => FlLine(
              color: AppTheme.border,
              strokeWidth: 1,
            ),
          ),
          borderData: FlBorderData(show: false),
          barGroups: companies.asMap().entries.map((entry) {
            return BarChartGroupData(
              x: entry.key,
              barRods: [
                BarChartRodData(
                  toY: entry.value.count.toDouble(),
                  color: AppTheme.primary,
                  width: 28,
                  borderRadius: const BorderRadius.vertical(top: Radius.circular(6)),
                ),
              ],
            );
          }).toList(),
        ),
      ),
    );
  }
}

class _TimelineChart extends StatelessWidget {
  final List<TimelinePoint> timeline;
  const _TimelineChart({required this.timeline});

  @override
  Widget build(BuildContext context) {
    final spots = timeline.asMap().entries.map((entry) {
      return FlSpot(entry.key.toDouble(), entry.value.count.toDouble());
    }).toList();

    final maxY = timeline.map((t) => t.count).reduce((a, b) => a > b ? a : b);

    return Container(
      height: 220,
      padding: const EdgeInsets.fromLTRB(8, 20, 24, 12),
      decoration: BoxDecoration(
        color: AppTheme.surface,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: AppTheme.border),
      ),
      child: LineChart(
        LineChartData(
          minY: 0,
          maxY: (maxY + 1).toDouble(),
          lineTouchData: LineTouchData(
            touchTooltipData: LineTouchTooltipData(
              getTooltipColor: (_) => AppTheme.surfaceVariant,
              getTooltipItems: (spots) => spots.map((spot) {
                final index = spot.x.toInt();
                final label = index < timeline.length
                    ? DateFormat('dd MMM').format(timeline[index].dateTime)
                    : '';
                return LineTooltipItem(
                  '$label\n${spot.y.toInt()} opportunity${spot.y > 1 ? 'ies' : 'y'}',
                  const TextStyle(
                    color: AppTheme.textPrimary,
                    fontSize: 12,
                    fontWeight: FontWeight.w500,
                  ),
                );
              }).toList(),
            ),
          ),
          titlesData: FlTitlesData(
            show: true,
            topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            leftTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 28,
                interval: 1,
                getTitlesWidget: (value, meta) {
                  if (value == value.floorToDouble() && value > 0) {
                    return Text(
                      value.toInt().toString(),
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 11,
                      ),
                    );
                  }
                  return const SizedBox.shrink();
                },
              ),
            ),
            bottomTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 32,
                interval: (timeline.length / 5).ceilToDouble().clamp(1, double.infinity),
                getTitlesWidget: (value, meta) {
                  final index = value.toInt();
                  if (index < 0 || index >= timeline.length) {
                    return const SizedBox.shrink();
                  }
                  return Padding(
                    padding: const EdgeInsets.only(top: 6),
                    child: Text(
                      DateFormat('dd MMM').format(timeline[index].dateTime),
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 10,
                      ),
                    ),
                  );
                },
              ),
            ),
          ),
          gridData: FlGridData(
            show: true,
            drawVerticalLine: false,
            getDrawingHorizontalLine: (_) => FlLine(
              color: AppTheme.border,
              strokeWidth: 1,
            ),
          ),
          borderData: FlBorderData(show: false),
          lineBarsData: [
            LineChartBarData(
              spots: spots,
              isCurved: true,
              curveSmoothness: 0.3,
              color: AppTheme.primary,
              barWidth: 2.5,
              isStrokeCapRound: true,
              dotData: FlDotData(
                show: true,
                getDotPainter: (spot, percent, bar, index) => FlDotCirclePainter(
                  radius: 4,
                  color: AppTheme.primary,
                  strokeWidth: 2,
                  strokeColor: AppTheme.background,
                ),
              ),
              belowBarData: BarAreaData(
                show: true,
                color: AppTheme.primary.withOpacity(0.08),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _BucketStatGrid extends StatelessWidget {
  final List<AnalyticsBucket> buckets;
  final bool isDesktop;
  final IconData icon;
  final Color color;

  const _BucketStatGrid({
    required this.buckets,
    required this.isDesktop,
    required this.icon,
    required this.color,
  });

  @override
  Widget build(BuildContext context) {
    return GridView.count(
      crossAxisCount: isDesktop ? 3 : 2,
      crossAxisSpacing: 12,
      mainAxisSpacing: 12,
      shrinkWrap: true,
      physics: const NeverScrollableScrollPhysics(),
      childAspectRatio: isDesktop ? 2.2 : 1.55,
      children: buckets
          .map(
            (bucket) => StatCard(
              label: bucket.label,
              value: bucket.count.toString(),
              icon: icon,
              color: color,
            ),
          )
          .toList(),
    );
  }
}

class _BucketBarChart extends StatelessWidget {
  final List<AnalyticsBucket> buckets;
  final Color barColor;

  const _BucketBarChart({
    required this.buckets,
    required this.barColor,
  });

  @override
  Widget build(BuildContext context) {
    final maxCount = buckets.map((bucket) => bucket.count).reduce((a, b) => a > b ? a : b);

    return Container(
      height: 260,
      padding: const EdgeInsets.fromLTRB(8, 20, 24, 12),
      decoration: BoxDecoration(
        color: AppTheme.surface,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: AppTheme.border),
      ),
      child: BarChart(
        BarChartData(
          alignment: BarChartAlignment.spaceAround,
          maxY: (maxCount + 1).toDouble(),
          barTouchData: BarTouchData(
            touchTooltipData: BarTouchTooltipData(
              getTooltipColor: (_) => AppTheme.surfaceVariant,
              getTooltipItem: (group, groupIndex, rod, rodIndex) {
                return BarTooltipItem(
                  '${buckets[groupIndex].label}\n',
                  const TextStyle(
                    color: AppTheme.textPrimary,
                    fontWeight: FontWeight.w600,
                    fontSize: 12,
                  ),
                  children: [
                    TextSpan(
                      text: '${rod.toY.toInt()} role${rod.toY > 1 ? 's' : ''}',
                      style: TextStyle(
                        color: barColor,
                        fontWeight: FontWeight.w500,
                        fontSize: 12,
                      ),
                    ),
                  ],
                );
              },
            ),
          ),
          titlesData: FlTitlesData(
            show: true,
            topTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            rightTitles: const AxisTitles(sideTitles: SideTitles(showTitles: false)),
            leftTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 28,
                interval: 1,
                getTitlesWidget: (value, meta) {
                  if (value == value.floorToDouble() && value > 0) {
                    return Text(
                      value.toInt().toString(),
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 11,
                      ),
                    );
                  }
                  return const SizedBox.shrink();
                },
              ),
            ),
            bottomTitles: AxisTitles(
              sideTitles: SideTitles(
                showTitles: true,
                reservedSize: 38,
                getTitlesWidget: (value, meta) {
                  final index = value.toInt();
                  if (index < 0 || index >= buckets.length) {
                    return const SizedBox.shrink();
                  }
                  final label = buckets[index].label;
                  final short = label.length > 12 ? '${label.substring(0, 12)}…' : label;
                  return Padding(
                    padding: const EdgeInsets.only(top: 6),
                    child: Text(
                      short,
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 10,
                      ),
                      textAlign: TextAlign.center,
                    ),
                  );
                },
              ),
            ),
          ),
          gridData: FlGridData(
            show: true,
            drawVerticalLine: false,
            getDrawingHorizontalLine: (_) => FlLine(
              color: AppTheme.border,
              strokeWidth: 1,
            ),
          ),
          borderData: FlBorderData(show: false),
          barGroups: buckets.asMap().entries.map((entry) {
            return BarChartGroupData(
              x: entry.key,
              barRods: [
                BarChartRodData(
                  toY: entry.value.count.toDouble(),
                  color: barColor,
                  width: 28,
                  borderRadius: const BorderRadius.vertical(top: Radius.circular(6)),
                ),
              ],
            );
          }).toList(),
        ),
      ),
    );
  }
}