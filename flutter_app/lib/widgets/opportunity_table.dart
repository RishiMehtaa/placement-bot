// import 'package:flutter/material.dart';
// import 'package:flutter_riverpod/flutter_riverpod.dart';
// import 'package:go_router/go_router.dart';
// import 'package:intl/intl.dart';
// import '../core/theme.dart';
// import '../models/opportunity.dart';
// import '../providers/analytics_provider.dart';
// import '../providers/opportunities_provider.dart';
// import '../services/api_service.dart';

// class OpportunityTable extends ConsumerStatefulWidget {
//   final List<Opportunity> opportunities;

//   const OpportunityTable({super.key, required this.opportunities});

//   @override
//   ConsumerState<OpportunityTable> createState() => _OpportunityTableState();
// }

// class _OpportunityTableState extends ConsumerState<OpportunityTable> {
//   final Set<String> _updatingRows = <String>{};
//   final Map<String, bool> _optimisticApplied = <String, bool>{};

//   String _rowKey(Opportunity opp) {
//     final normalizedRole = (opp.role ?? '').trim().toLowerCase();
//     return '${opp.id}|$normalizedRole';
//   }

//   Future<void> _toggleApplied(Opportunity opp, bool nextValue) async {
//     final role = (opp.role ?? '').trim();
//     if (role.isEmpty) return;

//     final key = _rowKey(opp);
//     setState(() {
//       _updatingRows.add(key);
//       _optimisticApplied[key] = nextValue;
//     });

//     try {
//       await ApiService().updateOpportunityApplied(
//         opportunityId: opp.id,
//         role: role,
//         applied: nextValue,
//       );
//       if (!mounted) return;

//       ref.invalidate(rawOpportunitiesProvider);
//       ref.invalidate(opportunitiesProvider);
//       ref.invalidate(analyticsSummaryProvider);
//       ref.invalidate(analyticsDataProvider);

//       ScaffoldMessenger.of(context).showSnackBar(
//         SnackBar(
//           content: Text(nextValue ? 'Marked as applied' : 'Removed from applied'),
//           duration: const Duration(milliseconds: 1200),
//         ),
//       );
//     } catch (e) {
//       if (!mounted) return;
//       ScaffoldMessenger.of(context).showSnackBar(
//         SnackBar(
//           content: Text('Failed to update applied status: $e'),
//           backgroundColor: AppTheme.error,
//         ),
//       );
//     } finally {
//       if (!mounted) return;
//       setState(() {
//         _updatingRows.remove(key);
//         _optimisticApplied.remove(key);
//       });
//     }
//   }

//   @override
//   Widget build(BuildContext context) {
//     final opportunities = widget.opportunities;

//     if (opportunities.isEmpty) {
//       return Center(
//         child: Column(
//           mainAxisAlignment: MainAxisAlignment.center,
//           children: [
//             Icon(Icons.inbox_rounded, size: 48, color: AppTheme.textSecondary.withOpacity(0.5)),
//             const SizedBox(height: 16),
//             const Text(
//               'No opportunities found',
//               style: TextStyle(color: AppTheme.textSecondary, fontSize: 15),
//             ),
//           ],
//         ),
//       );
//     }

//     return SingleChildScrollView(
//       scrollDirection: Axis.horizontal,
//       child: Expanded(
//         child: DataTable(
//           headingRowColor: WidgetStateProperty.all(AppTheme.surfaceVariant),
//           dataRowColor: WidgetStateProperty.resolveWith((states) {
//             if (states.contains(WidgetState.hovered)) return AppTheme.surfaceVariant;
//             return AppTheme.surface;
//           }),
//           border: TableBorder.all(color: AppTheme.border, width: 1),
//           columnSpacing: MediaQuery.of(context).size.width * 0.085,
//           headingTextStyle: const TextStyle(
//             color: AppTheme.textSecondary,
//             fontSize: 20,
//             fontWeight: FontWeight.w600,
//             letterSpacing: 0.5,
//           ),
//           columns: const [
//             DataColumn(label: Text('COMPANY')),
//             DataColumn(label: Text('ROLE')),
//             DataColumn(label: Text('DEADLINE')),
//             DataColumn(label: Text('PACKAGE')),
//             DataColumn(label: Text('APPLIED')),
//             DataColumn(label: Text('CONFIDENCE')),
//             DataColumn(label: Text('ACTION')),
//           ],
//           rows: opportunities.map((opp) {
//             final deadlineDate = opp.deadlineDate;
//             final deadlineStr = deadlineDate != null
//                 ? DateFormat('dd MMM yy').format(deadlineDate)
//                 : '—';
//             final key = _rowKey(opp);
//             final isUpdating = _updatingRows.contains(key);
//             final appliedValue = _optimisticApplied[key] ?? opp.applied;
//             Color deadlineColor = AppTheme.textPrimary;
//             if (opp.isDeadlinePast) deadlineColor = AppTheme.error;
//             if (opp.isDeadlineSoon) deadlineColor = AppTheme.warning;
        
//             return DataRow(
//               cells: [
//                 DataCell(
//                   Row(
//                     mainAxisSize: MainAxisSize.max,
//                     children: [
//                       Container(
//                         width: 28,
//                         height: 28,
//                         decoration: BoxDecoration(
//                           color: AppTheme.primary.withOpacity(0.15),
//                           borderRadius: BorderRadius.circular(6),
//                         ),
//                         child: Center(
//                           child: Text(
//                             (opp.company ?? '?')[0].toUpperCase(),
//                             style: const TextStyle(
//                               color: AppTheme.primary,
//                               fontWeight: FontWeight.w700,
//                               fontSize: 12,
//                             ),
//                           ),
//                         ),
//                       ),
//                       const SizedBox(width: 10),
//                       ConstrainedBox(
//                         constraints: const BoxConstraints(maxWidth: 140),
//                         child: Text(
//                           opp.company ?? '—',
//                           style: const TextStyle(
//                             color: AppTheme.textPrimary,
//                             fontWeight: FontWeight.w500,
//                             fontSize: 13,
//                           ),
//                           overflow: TextOverflow.ellipsis,
//                         ),
//                       ),
//                     ],
//                   ),
//                 ),
//                 DataCell(
//                   ConstrainedBox(
//                     constraints: const BoxConstraints(maxWidth: 160),
//                     child: Text(
//                       opp.role ?? '—',
//                       style: const TextStyle(color: AppTheme.textPrimary, fontSize: 13),
//                       overflow: TextOverflow.ellipsis,
//                     ),
//                   ),
//                 ),
//                 DataCell(
//                   Text(
//                     deadlineStr,
//                     style: TextStyle(
//                       color: deadlineColor,
//                       fontSize: 13,
//                       fontWeight: FontWeight.w500,
//                     ),
//                   ),
//                 ),
//                 DataCell(
//                   Text(
//                     opp.package ?? '—',
//                     style: const TextStyle(color: AppTheme.accent, fontSize: 13),
//                   ),
//                 ),
//                 DataCell(
//                   SizedBox(
//                     width: 52,
//                     child: Center(
//                       child: Checkbox(
//                         value: appliedValue,
//                         onChanged: isUpdating || (opp.role ?? '').trim().isEmpty
//                             ? null
//                             : (value) {
//                                 if (value == null) return;
//                                 _toggleApplied(opp, value);
//                               },
//                         activeColor: AppTheme.primary,
//                       ),
//                     ),
//                   ),
//                 ),
//                 DataCell(
//                   Container(
//                     padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
//                     decoration: BoxDecoration(
//                       color: AppTheme.surfaceVariant,
//                       borderRadius: BorderRadius.circular(6),
//                     ),
//                     child: Text(
//                       opp.confidence != null
//                           ? '${(opp.confidence! * 100).toStringAsFixed(0)}%'
//                           : '—',
//                       style: const TextStyle(
//                         color: AppTheme.textSecondary,
//                         fontSize: 12,
//                       ),
//                     ),
//                   ),
//                 ),
//                 DataCell(
//                   TextButton(
//                     onPressed: () => context.go('/opportunity/${opp.id}'),
//                     style: TextButton.styleFrom(
//                       foregroundColor: AppTheme.primary,
//                       padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
//                     ),
//                     child: const Text('View', style: TextStyle(fontSize: 13)),
//                   ),
//                 ),
//               ],
//             );
//           }).toList(),
//         ),
//       ),
//     );
//   }
// }


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

    return Container(
      decoration: BoxDecoration(
        color: AppTheme.surface,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: AppTheme.border),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          // Header row
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
            decoration: const BoxDecoration(
              color: AppTheme.surfaceVariant,
              borderRadius: BorderRadius.only(
                topLeft: Radius.circular(12),
                topRight: Radius.circular(12),
              ),
            ),
            child: const Row(
              children: [
                Expanded(flex: 3, child: _HeaderCell(label: 'COMPANY')),
                Expanded(flex: 3, child: _HeaderCell(label: 'ROLE')),
                Expanded(flex: 2, child: _HeaderCell(label: 'DEADLINE')),
                Expanded(flex: 2, child: _HeaderCell(label: 'PACKAGE')),
                Expanded(flex: 1, child: _HeaderCell(label: 'APPLIED')),
                Expanded(flex: 1, child: _HeaderCell(label: 'CONF.')),
                Expanded(flex: 1, child: _HeaderCell(label: '')),
              ],
            ),
          ),

          // Data rows
          ListView.separated(
            shrinkWrap: true,
            physics: const NeverScrollableScrollPhysics(),
            itemCount: opportunities.length,
            separatorBuilder: (_, __) => const Divider(height: 1, color: AppTheme.border),
            itemBuilder: (context, index) {
              final opp = opportunities[index];
              final key = _rowKey(opp);
              final isUpdating = _updatingRows.contains(key);
              final appliedValue = _optimisticApplied[key] ?? opp.applied;
              return _TableRow(
                opportunity: opp,
                isUpdating: isUpdating,
                appliedValue: appliedValue,
                onToggleApplied: _toggleApplied,
              );
            },
          ),
        ],
      ),
    );
  }
}

class _HeaderCell extends StatelessWidget {
  final String label;
  const _HeaderCell({required this.label});

  @override
  Widget build(BuildContext context) {
    return Text(
      label,
      style: const TextStyle(
        color: AppTheme.textSecondary,
        fontSize: 11,
        fontWeight: FontWeight.w600,
        letterSpacing: 0.8,
      ),
    );
  }
}

class _TableRow extends StatefulWidget {
  final Opportunity opportunity;
  final bool isUpdating;
  final bool appliedValue;
  final Future<void> Function(Opportunity, bool) onToggleApplied;

  const _TableRow({
    required this.opportunity,
    required this.isUpdating,
    required this.appliedValue,
    required this.onToggleApplied,
  });

  @override
  State<_TableRow> createState() => _TableRowState();
}

class _TableRowState extends State<_TableRow> {
  bool _hovered = false;

  @override
  Widget build(BuildContext context) {
    final opp = widget.opportunity;
    final deadlineDate = opp.deadlineDate;
    final deadlineStr = deadlineDate != null
        ? DateFormat('dd MMM yy').format(deadlineDate)
        : '—';

    Color deadlineColor = AppTheme.textPrimary;
    if (opp.isDeadlinePast) deadlineColor = AppTheme.error;
    if (opp.isDeadlineSoon) deadlineColor = AppTheme.warning;

    return MouseRegion(
      onEnter: (_) => setState(() => _hovered = true),
      onExit: (_) => setState(() => _hovered = false),
      child: GestureDetector(
        onTap: () => context.go('/opportunity/${opp.id}'),
        child: AnimatedContainer(
          duration: const Duration(milliseconds: 150),
          color: _hovered ? AppTheme.surfaceVariant : Colors.transparent,
          padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 14),
          child: Row(
            children: [
              // Company
              Expanded(
                flex: 3,
                child: Row(
                  children: [
                    Container(
                      width: 30,
                      height: 30,
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
                    Expanded(
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

              // Role
              Expanded(
                flex: 3,
                child: Text(
                  opp.role ?? '—',
                  style: const TextStyle(
                    color: AppTheme.textSecondary,
                    fontSize: 13,
                  ),
                  overflow: TextOverflow.ellipsis,
                ),
              ),

              // Deadline
              Expanded(
                flex: 2,
                child: Row(
                  children: [
                    Icon(Icons.calendar_today, size: 11, color: deadlineColor),
                    const SizedBox(width: 4),
                    Text(
                      deadlineStr,
                      style: TextStyle(
                        color: deadlineColor,
                        fontSize: 12,
                        fontWeight: FontWeight.w500,
                      ),
                    ),
                  ],
                ),
              ),

              // Package
              Expanded(
                flex: 2,
                child: Text(
                  opp.package ?? '—',
                  style: const TextStyle(color: AppTheme.accent, fontSize: 12),
                  overflow: TextOverflow.ellipsis,
                ),
              ),

              // Applied checkbox
              Expanded(
                flex: 1,
                child: Center(
                  child: Checkbox(
                    value: widget.appliedValue,
                    onChanged: widget.isUpdating || (opp.role ?? '').trim().isEmpty
                        ? null
                        : (value) {
                            if (value == null) return;
                            widget.onToggleApplied(opp, value);
                          },
                    activeColor: AppTheme.primary,
                  ),
                ),
              ),

              // Confidence
              Expanded(
                flex: 1,
                child: Center(
                  child: Container(
                    padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 3),
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
                        fontSize: 11,
                      ),
                      textAlign: TextAlign.center,
                    ),
                  ),
                ),
              ),

              // Arrow
              Expanded(
                flex: 1,
                child: Align(
                  alignment: Alignment.centerRight,
                  child: Icon(
                    Icons.arrow_forward_ios,
                    size: 12,
                    color: _hovered ? AppTheme.primary : AppTheme.textSecondary,
                  ),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
