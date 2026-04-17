import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'package:intl/intl.dart';
import '../core/theme.dart';
import '../models/opportunity.dart';
import '../providers/analytics_provider.dart';
import '../providers/opportunities_provider.dart';
import '../services/api_service.dart';

class OpportunityTable extends ConsumerStatefulWidget {
  final List<Opportunity> opportunities;

  const OpportunityTable({super.key, required this.opportunities});

  @override
  ConsumerState<OpportunityTable> createState() => _OpportunityTableState();
}

class _OpportunityTableState extends ConsumerState<OpportunityTable> {
  final Set<String> _updatingRows = <String>{};
  final Map<String, bool> _optimisticApplied = <String, bool>{};

  String _rowKey(Opportunity opp) {
    final normalizedRole = (opp.role ?? '').trim().toLowerCase();
    return '${opp.id}|$normalizedRole';
  }

  Future<void> _toggleApplied(Opportunity opp, bool nextValue) async {
    final role = (opp.role ?? '').trim();
    if (role.isEmpty) return;

    final key = _rowKey(opp);
    setState(() {
      _updatingRows.add(key);
      _optimisticApplied[key] = nextValue;
    });

    try {
      await ApiService().updateOpportunityApplied(
        opportunityId: opp.id,
        role: role,
        applied: nextValue,
      );
      if (!mounted) return;

      ref.invalidate(rawOpportunitiesProvider);
      ref.invalidate(opportunitiesProvider);
      ref.invalidate(analyticsSummaryProvider);
      ref.invalidate(analyticsDataProvider);

      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(nextValue ? 'Marked as applied' : 'Removed from applied'),
          duration: const Duration(milliseconds: 1200),
        ),
      );
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Failed to update applied status: $e'),
          backgroundColor: AppTheme.error,
        ),
      );
    } finally {
      if (!mounted) return;
      setState(() {
        _updatingRows.remove(key);
        _optimisticApplied.remove(key);
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    final opportunities = widget.opportunities;

    if (opportunities.isEmpty) {
      return Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(Icons.inbox_rounded, size: 48, color: AppTheme.textSecondary.withOpacity(0.5)),
            const SizedBox(height: 16),
            const Text(
              'No opportunities found',
              style: TextStyle(color: AppTheme.textSecondary, fontSize: 15),
            ),
          ],
        ),
      );
    }

    return SingleChildScrollView(
      scrollDirection: Axis.horizontal,
      child: Expanded(
        child: DataTable(
          headingRowColor: WidgetStateProperty.all(AppTheme.surfaceVariant),
          dataRowColor: WidgetStateProperty.resolveWith((states) {
            if (states.contains(WidgetState.hovered)) return AppTheme.surfaceVariant;
            return AppTheme.surface;
          }),
          border: TableBorder.all(color: AppTheme.border, width: 1),
          columnSpacing: MediaQuery.of(context).size.width * 0.085,
          headingTextStyle: const TextStyle(
            color: AppTheme.textSecondary,
            fontSize: 20,
            fontWeight: FontWeight.w600,
            letterSpacing: 0.5,
          ),
          columns: const [
            DataColumn(label: Text('COMPANY')),
            DataColumn(label: Text('ROLE')),
            DataColumn(label: Text('DEADLINE')),
            DataColumn(label: Text('PACKAGE')),
            DataColumn(label: Text('APPLIED')),
            DataColumn(label: Text('CONFIDENCE')),
            DataColumn(label: Text('ACTION')),
          ],
          rows: opportunities.map((opp) {
            final deadlineDate = opp.deadlineDate;
            final deadlineStr = deadlineDate != null
                ? DateFormat('dd MMM yy').format(deadlineDate)
                : '—';
            final key = _rowKey(opp);
            final isUpdating = _updatingRows.contains(key);
            final appliedValue = _optimisticApplied[key] ?? opp.applied;
            Color deadlineColor = AppTheme.textPrimary;
            if (opp.isDeadlinePast) deadlineColor = AppTheme.error;
            if (opp.isDeadlineSoon) deadlineColor = AppTheme.warning;
        
            return DataRow(
              cells: [
                DataCell(
                  Row(
                    mainAxisSize: MainAxisSize.max,
                    children: [
                      Container(
                        width: 28,
                        height: 28,
                        decoration: BoxDecoration(
                          color: AppTheme.primary.withOpacity(0.15),
                          borderRadius: BorderRadius.circular(6),
                        ),
                        child: Center(
                          child: Text(
                            (opp.company ?? '?')[0].toUpperCase(),
                            style: const TextStyle(
                              color: AppTheme.primary,
                              fontWeight: FontWeight.w700,
                              fontSize: 12,
                            ),
                          ),
                        ),
                      ),
                      const SizedBox(width: 10),
                      ConstrainedBox(
                        constraints: const BoxConstraints(maxWidth: 140),
                        child: Text(
                          opp.company ?? '—',
                          style: const TextStyle(
                            color: AppTheme.textPrimary,
                            fontWeight: FontWeight.w500,
                            fontSize: 13,
                          ),
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                    ],
                  ),
                ),
                DataCell(
                  ConstrainedBox(
                    constraints: const BoxConstraints(maxWidth: 160),
                    child: Text(
                      opp.role ?? '—',
                      style: const TextStyle(color: AppTheme.textPrimary, fontSize: 13),
                      overflow: TextOverflow.ellipsis,
                    ),
                  ),
                ),
                DataCell(
                  Text(
                    deadlineStr,
                    style: TextStyle(
                      color: deadlineColor,
                      fontSize: 13,
                      fontWeight: FontWeight.w500,
                    ),
                  ),
                ),
                DataCell(
                  Text(
                    opp.package ?? '—',
                    style: const TextStyle(color: AppTheme.accent, fontSize: 13),
                  ),
                ),
                DataCell(
                  SizedBox(
                    width: 52,
                    child: Center(
                      child: Checkbox(
                        value: appliedValue,
                        onChanged: isUpdating || (opp.role ?? '').trim().isEmpty
                            ? null
                            : (value) {
                                if (value == null) return;
                                _toggleApplied(opp, value);
                              },
                        activeColor: AppTheme.primary,
                      ),
                    ),
                  ),
                ),
                DataCell(
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
                    decoration: BoxDecoration(
                      color: AppTheme.surfaceVariant,
                      borderRadius: BorderRadius.circular(6),
                    ),
                    child: Text(
                      opp.confidence != null
                          ? '${(opp.confidence! * 100).toStringAsFixed(0)}%'
                          : '—',
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 12,
                      ),
                    ),
                  ),
                ),
                DataCell(
                  TextButton(
                    onPressed: () => context.go('/opportunity/${opp.id}'),
                    style: TextButton.styleFrom(
                      foregroundColor: AppTheme.primary,
                      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
                    ),
                    child: const Text('View', style: TextStyle(fontSize: 13)),
                  ),
                ),
              ],
            );
          }).toList(),
        ),
      ),
    );
  }
}