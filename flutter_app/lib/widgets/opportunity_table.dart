import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'package:intl/intl.dart';
import '../core/theme.dart';
import '../models/opportunity.dart';

class OpportunityTable extends StatelessWidget {
  final List<Opportunity> opportunities;

  const OpportunityTable({super.key, required this.opportunities});

  @override
  Widget build(BuildContext context) {
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
            DataColumn(label: Text('CONFIDENCE')),
            DataColumn(label: Text('ACTION')),
          ],
          rows: opportunities.map((opp) {
            final deadlineDate = opp.deadlineDate;
            final deadlineStr = deadlineDate != null
                ? DateFormat('dd MMM yy').format(deadlineDate)
                : '—';
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