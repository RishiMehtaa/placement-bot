import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'package:intl/intl.dart';
import '../core/theme.dart';
import '../models/opportunity.dart';

class OpportunityCard extends StatelessWidget {
  final Opportunity opportunity;

  const OpportunityCard({super.key, required this.opportunity});

  @override
  Widget build(BuildContext context) {
    final deadlineDate = opportunity.deadlineDate;
    final deadlineStr = deadlineDate != null
        ? DateFormat('dd MMM yyyy').format(deadlineDate)
        : 'No deadline';

    Color deadlineColor = AppTheme.textSecondary;
    if (opportunity.isDeadlinePast) deadlineColor = AppTheme.error;
    if (opportunity.isDeadlineSoon) deadlineColor = AppTheme.warning;

    return GestureDetector(
      onTap: () => context.go('/opportunity/${opportunity.id}'),
      child: Container(
        padding: const EdgeInsets.all(16),
        decoration: BoxDecoration(
          color: AppTheme.surface,
          borderRadius: BorderRadius.circular(12),
          border: Border.all(color: AppTheme.border),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                // Company avatar
                Container(
                  width: 40,
                  height: 40,
                  decoration: BoxDecoration(
                    color: AppTheme.primary.withOpacity(0.15),
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: Center(
                    child: Text(
                      (opportunity.company ?? '?')[0].toUpperCase(),
                      style: const TextStyle(
                        color: AppTheme.primary,
                        fontWeight: FontWeight.w700,
                        fontSize: 16,
                      ),
                    ),
                  ),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        opportunity.company ?? 'Unknown Company',
                        style: const TextStyle(
                          fontSize: 15,
                          fontWeight: FontWeight.w600,
                          color: AppTheme.textPrimary,
                        ),
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                      ),
                      const SizedBox(height: 2),
                      Text(
                        opportunity.role ?? 'Role not specified',
                        style: const TextStyle(
                          fontSize: 13,
                          color: AppTheme.textSecondary,
                        ),
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                      ),
                    ],
                  ),
                ),
                const Icon(
                  Icons.chevron_right,
                  color: AppTheme.textSecondary,
                  size: 20,
                ),
              ],
            ),
            const SizedBox(height: 14),
            const Divider(color: AppTheme.border, height: 1),
            const SizedBox(height: 14),
            Row(
              children: [
                _InfoChip(
                  icon: Icons.calendar_today,
                  label: deadlineStr,
                  color: deadlineColor,
                ),
                const SizedBox(width: 8),
                if (opportunity.package != null)
                  _InfoChip(
                    icon: Icons.currency_rupee,
                    label: opportunity.package!,
                    color: AppTheme.accent,
                  ),
                const Spacer(),
                if (opportunity.confidence != null)
                  Container(
                    padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
                    decoration: BoxDecoration(
                      color: AppTheme.surfaceVariant,
                      borderRadius: BorderRadius.circular(6),
                    ),
                    child: Text(
                      '${(opportunity.confidence! * 100).toStringAsFixed(0)}%',
                      style: const TextStyle(
                        fontSize: 11,
                        color: AppTheme.textSecondary,
                        fontWeight: FontWeight.w500,
                      ),
                    ),
                  ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}

class _InfoChip extends StatelessWidget {
  final IconData icon;
  final String label;
  final Color color;

  const _InfoChip({
    required this.icon,
    required this.label,
    required this.color,
  });

  @override
  Widget build(BuildContext context) {
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        Icon(icon, size: 12, color: color),
        const SizedBox(width: 4),
        Text(
          label,
          style: TextStyle(
            fontSize: 12,
            color: color,
            fontWeight: FontWeight.w500,
          ),
        ),
      ],
    );
  }
}