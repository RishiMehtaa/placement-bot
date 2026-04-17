import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:go_router/go_router.dart';
import 'package:intl/intl.dart';
import 'package:url_launcher/url_launcher.dart';
import '../core/theme.dart';
import '../models/opportunity_detail.dart';
import '../providers/opportunities_provider.dart';

class OpportunityDetailScreen extends ConsumerWidget {
  final String familyId;

  const OpportunityDetailScreen({super.key, required this.familyId});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final detailAsync = ref.watch(opportunityDetailProvider(familyId));
    final isDesktop = MediaQuery.of(context).size.width >= 768;

    return Scaffold(
      appBar: AppBar(
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () => context.go('/dashboard'),
        ),
        title: const Text('Opportunity Detail'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: () => ref.invalidate(opportunityDetailProvider(familyId)),
          ),
          const SizedBox(width: 8),
        ],
      ),
      body: detailAsync.when(
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
                onPressed: () => ref.invalidate(opportunityDetailProvider(familyId)),
                child: const Text('Retry'),
              ),
            ],
          ),
        ),
        data: (raw) {
          final detail = OpportunityDetail.fromJson(raw);
          return SingleChildScrollView(
            padding: EdgeInsets.all(isDesktop ? 32 : 16),
            child: isDesktop
                ? Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Expanded(
                        flex: 3,
                        child: _MainColumn(detail: detail),
                      ),
                      const SizedBox(width: 24),
                      Expanded(
                        flex: 2,
                        child: _SideColumn(detail: detail),
                      ),
                    ],
                  )
                : Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      _MainColumn(detail: detail),
                      const SizedBox(height: 24),
                      _SideColumn(detail: detail),
                    ],
                  ),
          );
        },
      ),
    );
  }
}

class _MainColumn extends StatelessWidget {
  final OpportunityDetail detail;
  const _MainColumn({required this.detail});

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _CompanyHeader(detail: detail),
        const SizedBox(height: 24),
        _InfoGrid(detail: detail),
        const SizedBox(height: 24),
        if (detail.jdLink != null) ...[
          _ApplyButton(url: detail.jdLink!),
          const SizedBox(height: 24),
        ],
        if (detail.notes.isNotEmpty) ...[
          _NotesSection(notes: detail.notes),
          const SizedBox(height: 24),
        ],
        _MessagesSection(messages: detail.messages),
      ],
    );
  }
}

class _SideColumn extends StatelessWidget {
  final OpportunityDetail detail;
  const _SideColumn({required this.detail});

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        _DeadlineCountdown(detail: detail),
        const SizedBox(height: 16),
        _MetaCard(detail: detail),
      ],
    );
  }
}

class _CompanyHeader extends StatelessWidget {
  final OpportunityDetail detail;
  const _CompanyHeader({required this.detail});

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        Container(
          width: 56,
          height: 56,
          decoration: BoxDecoration(
            color: AppTheme.primary.withOpacity(0.15),
            borderRadius: BorderRadius.circular(14),
          ),
          child: Center(
            child: Text(
              (detail.company ?? '?')[0].toUpperCase(),
              style: const TextStyle(
                color: AppTheme.primary,
                fontWeight: FontWeight.w700,
                fontSize: 22,
              ),
            ),
          ),
        ),
        const SizedBox(width: 16),
        Expanded(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                detail.company ?? 'Unknown Company',
                style: const TextStyle(
                  fontSize: 20,
                  fontWeight: FontWeight.w700,
                  color: AppTheme.textPrimary,
                ),
              ),
              const SizedBox(height: 4),
              Text(
                detail.role ?? 'Role not specified',
                style: const TextStyle(
                  fontSize: 14,
                  color: AppTheme.textSecondary,
                ),
              ),
            ],
          ),
        ),
      ],
    );
  }
}

class _InfoGrid extends StatelessWidget {
  final OpportunityDetail detail;
  const _InfoGrid({required this.detail});

  @override
  Widget build(BuildContext context) {
    final deadlineDate = detail.deadlineDate;
    final deadlineStr = deadlineDate != null
        ? DateFormat('dd MMM yyyy, hh:mm a').format(deadlineDate.toLocal())
        : 'Not specified';

    Color deadlineColor = AppTheme.textPrimary;
    if (detail.isDeadlinePast) deadlineColor = AppTheme.error;
    if (detail.isDeadlineSoon) deadlineColor = AppTheme.warning;

    return Container(
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: AppTheme.surface,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: AppTheme.border),
      ),
      child: Column(
        children: [
          _InfoRow(
            icon: Icons.business_rounded,
            label: 'Company',
            value: detail.company ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.work_outline_rounded,
            label: 'Role',
            value: detail.role ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.verified_user_outlined,
            label: 'Eligible',
            value: detail.eligible ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.rule_rounded,
            label: 'Eligibility Reason',
            value: detail.eligibleReason ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.location_on_outlined,
            label: 'Location',
            value: detail.location ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.schedule_outlined,
            label: 'Duration',
            value: detail.duration ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.event_available_outlined,
            label: 'Start Date',
            value: detail.startDate ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.link_outlined,
            label: 'Internal Form Link',
            value: detail.internalFormLink ?? '—',
            valueColor: AppTheme.textPrimary,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.calendar_today_rounded,
            label: 'Deadline',
            value: deadlineStr,
            valueColor: deadlineColor,
          ),
          _Divider(),
          _InfoRow(
            icon: Icons.currency_rupee_rounded,
            label: 'Package / Stipend',
            value: detail.package ?? '—',
            valueColor: AppTheme.accent,
          ),
          if (detail.confidence != null) ...[
            _Divider(),
            _InfoRow(
              icon: Icons.verified_outlined,
              label: 'Confidence',
              value: '${(detail.confidence! * 100).toStringAsFixed(0)}%',
              valueColor: AppTheme.textSecondary,
            ),
          ],
        ],
      ),
    );
  }
}

class _InfoRow extends StatelessWidget {
  final IconData icon;
  final String label;
  final String value;
  final Color valueColor;

  const _InfoRow({
    required this.icon,
    required this.label,
    required this.value,
    required this.valueColor,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 12),
      child: Row(
        children: [
          Icon(icon, size: 16, color: AppTheme.textSecondary),
          const SizedBox(width: 12),
          SizedBox(
            width: 120,
            child: Text(
              label,
              style: const TextStyle(
                fontSize: 13,
                color: AppTheme.textSecondary,
              ),
            ),
          ),
          Expanded(
            child: Text(
              value,
              style: TextStyle(
                fontSize: 13,
                color: valueColor,
                fontWeight: FontWeight.w500,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _Divider extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return const Divider(color: AppTheme.border, height: 1);
  }
}

class _ApplyButton extends StatelessWidget {
  final String url;
  const _ApplyButton({required this.url});

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: double.infinity,
      child: ElevatedButton.icon(
        onPressed: () async {
          final uri = Uri.tryParse(url);
          if (uri != null && await canLaunchUrl(uri)) {
            await launchUrl(uri, mode: LaunchMode.externalApplication);
          }
        },
        icon: const Icon(Icons.open_in_new, size: 16),
        label: const Text('Apply Now'),
        style: ElevatedButton.styleFrom(
          backgroundColor: AppTheme.primary,
          foregroundColor: Colors.white,
          padding: const EdgeInsets.symmetric(vertical: 14),
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(10),
          ),
        ),
      ),
    );
  }
}

class _NotesSection extends StatelessWidget {
  final List<String> notes;
  const _NotesSection({required this.notes});

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Text(
          'Notes',
          style: TextStyle(
            fontSize: 15,
            fontWeight: FontWeight.w600,
            color: AppTheme.textPrimary,
          ),
        ),
        const SizedBox(height: 12),
        Container(
          padding: const EdgeInsets.all(16),
          decoration: BoxDecoration(
            color: AppTheme.surface,
            borderRadius: BorderRadius.circular(12),
            border: Border.all(color: AppTheme.border),
          ),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: notes
                .map(
                  (note) => Padding(
                    padding: const EdgeInsets.only(bottom: 8),
                    child: Row(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        const Padding(
                          padding: EdgeInsets.only(top: 5),
                          child: CircleAvatar(
                            radius: 3,
                            backgroundColor: AppTheme.textSecondary,
                          ),
                        ),
                        const SizedBox(width: 10),
                        Expanded(
                          child: Text(
                            note,
                            style: const TextStyle(
                              fontSize: 13,
                              color: AppTheme.textSecondary,
                            ),
                          ),
                        ),
                      ],
                    ),
                  ),
                )
                .toList(),
          ),
        ),
      ],
    );
  }
}

class _MessagesSection extends StatelessWidget {
  final List<ContributingMessage> messages;
  const _MessagesSection({required this.messages});

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Contributing Messages (${messages.length})',
          style: const TextStyle(
            fontSize: 15,
            fontWeight: FontWeight.w600,
            color: AppTheme.textPrimary,
          ),
        ),
        const SizedBox(height: 12),
        if (messages.isEmpty)
          Container(
            padding: const EdgeInsets.all(16),
            decoration: BoxDecoration(
              color: AppTheme.surface,
              borderRadius: BorderRadius.circular(12),
              border: Border.all(color: AppTheme.border),
            ),
            child: const Text(
              'No messages linked to this opportunity.',
              style: TextStyle(color: AppTheme.textSecondary, fontSize: 13),
            ),
          )
        else
          ...messages.map((msg) => _MessageBubble(message: msg)),
      ],
    );
  }
}

class _MessageBubble extends StatelessWidget {
  final ContributingMessage message;
  const _MessageBubble({required this.message});

  @override
  Widget build(BuildContext context) {
    final ts = message.timestamp != null
        ? DateFormat('dd MMM yyyy, hh:mm a')
            .format(DateTime.parse(message.timestamp!).toLocal())
        : null;

    return Container(
      margin: const EdgeInsets.only(bottom: 12),
      padding: const EdgeInsets.all(14),
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
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 3),
                decoration: BoxDecoration(
                  color: AppTheme.primary.withOpacity(0.15),
                  borderRadius: BorderRadius.circular(6),
                ),
                child: Text(
                  message.contributionRole ?? 'message',
                  style: const TextStyle(
                    fontSize: 11,
                    color: AppTheme.primary,
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ),
              const Spacer(),
              if (ts != null)
                Text(
                  ts,
                  style: const TextStyle(
                    fontSize: 11,
                    color: AppTheme.textSecondary,
                  ),
                ),
              const SizedBox(width: 8),
              GestureDetector(
                onTap: () {
                  Clipboard.setData(ClipboardData(text: message.text));
                  ScaffoldMessenger.of(context).showSnackBar(
                    const SnackBar(
                      content: Text('Message copied'),
                      duration: Duration(seconds: 1),
                    ),
                  );
                },
                child: const Icon(
                  Icons.copy,
                  size: 14,
                  color: AppTheme.textSecondary,
                ),
              ),
            ],
          ),
          const SizedBox(height: 10),
          Text(
            message.text,
            style: const TextStyle(
              fontSize: 13,
              color: AppTheme.textPrimary,
              height: 1.5,
            ),
          ),
        ],
      ),
    );
  }
}

class _DeadlineCountdown extends StatelessWidget {
  final OpportunityDetail detail;
  const _DeadlineCountdown({required this.detail});

  @override
  Widget build(BuildContext context) {
    final remaining = detail.timeUntilDeadline;
    final isPast = detail.isDeadlinePast;
    final isSoon = detail.isDeadlineSoon;

    String countdownText;
    Color countdownColor;
    IconData countdownIcon;

    if (remaining == null) {
      countdownText = 'No deadline set';
      countdownColor = AppTheme.textSecondary;
      countdownIcon = Icons.calendar_today_outlined;
    } else if (isPast) {
      countdownText = 'Deadline passed';
      countdownColor = AppTheme.error;
      countdownIcon = Icons.timer_off_outlined;
    } else if (remaining.inDays > 0) {
      countdownText = '${remaining.inDays}d ${remaining.inHours % 24}h remaining';
      countdownColor = isSoon ? AppTheme.warning : AppTheme.accent;
      countdownIcon = Icons.timer_outlined;
    } else {
      countdownText = '${remaining.inHours}h ${remaining.inMinutes % 60}m remaining';
      countdownColor = AppTheme.error;
      countdownIcon = Icons.timer_outlined;
    }

    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: countdownColor.withOpacity(0.08),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: countdownColor.withOpacity(0.3)),
      ),
      child: Row(
        children: [
          Icon(countdownIcon, color: countdownColor, size: 20),
          const SizedBox(width: 12),
          Expanded(
            child: Text(
              countdownText,
              style: TextStyle(
                fontSize: 15,
                fontWeight: FontWeight.w600,
                color: countdownColor,
              ),
            ),
          ),
        ],
      ),
    );
  }
}

class _MetaCard extends StatelessWidget {
  final OpportunityDetail detail;
  const _MetaCard({required this.detail});

  @override
  Widget build(BuildContext context) {
    final createdAt = detail.createdAt != null
        ? DateFormat('dd MMM yyyy, hh:mm a')
            .format(DateTime.parse(detail.createdAt!).toLocal())
        : '—';
    final updatedAt = detail.updatedAt != null
        ? DateFormat('dd MMM yyyy, hh:mm a')
            .format(DateTime.parse(detail.updatedAt!).toLocal())
        : '—';
    final syncedAt = detail.lastSyncedAt != null
        ? DateFormat('dd MMM yyyy, hh:mm a')
            .format(DateTime.parse(detail.lastSyncedAt!).toLocal())
        : '—';

    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.surface,
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: AppTheme.border),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text(
            'Metadata',
            style: TextStyle(
              fontSize: 13,
              fontWeight: FontWeight.w600,
              color: AppTheme.textSecondary,
              letterSpacing: 0.5,
            ),
          ),
          const SizedBox(height: 12),
          _MetaRow(label: 'Family ID', value: detail.id.substring(0, 8) + '...'),
          _MetaRow(label: 'Created', value: createdAt),
          _MetaRow(label: 'Updated', value: updatedAt),
          _MetaRow(label: 'Sheets Sync', value: detail.syncStatus ?? '—'),
          _MetaRow(label: 'Last Synced', value: syncedAt),
        ],
      ),
    );
  }
}

class _MetaRow extends StatelessWidget {
  final String label;
  final String value;
  const _MetaRow({required this.label, required this.value});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(
            width: 90,
            child: Text(
              label,
              style: const TextStyle(
                fontSize: 12,
                color: AppTheme.textSecondary,
              ),
            ),
          ),
          Expanded(
            child: Text(
              value,
              style: const TextStyle(
                fontSize: 12,
                color: AppTheme.textPrimary,
                fontWeight: FontWeight.w500,
              ),
            ),
          ),
        ],
      ),
    );
  }
}