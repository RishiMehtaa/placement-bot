import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import '../core/theme.dart';
import 'package:super_icons/super_icons.dart';

class WelcomeScreen extends StatelessWidget {
  const WelcomeScreen({super.key});

  @override
  Widget build(BuildContext context) {
    final size = MediaQuery.of(context).size;
    final isDesktop = size.width >= 768;

    return Scaffold(
      backgroundColor: AppTheme.background,
      body: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 480),
          child: Padding(
            padding: EdgeInsets.symmetric(
              horizontal: isDesktop ? 48 : 32,
              vertical: 48,
            ),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              crossAxisAlignment: CrossAxisAlignment.center,
              children: [
                // Logo / Icon
                Container(
                  width: 96,
                  height: 96,
                  decoration: BoxDecoration(
                    color: AppTheme.primary,
                    borderRadius: BorderRadius.circular(24),
                    border: Border.all(
                      color: AppTheme.primary,
                      width: 1.5,
                    ),
                  ),
                  child: const Icon(
                    SuperIcons.bs_whatsapp,
                    size: 48,
                    color: AppTheme.background,
                  ),
                ),
                const SizedBox(height: 40),

                // App name
                const Text(
                  'Placement Intelligence',
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    fontSize: 28,
                    fontWeight: FontWeight.w700,
                    color: AppTheme.textPrimary,
                    letterSpacing: -0.5,
                  ),
                ),
                const SizedBox(height: 16),

                // Tagline
                const Text(
                  'Connect your placement WhatsApp group and track every opportunity automatically.',
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    fontSize: 16,
                    color: AppTheme.textSecondary,
                    height: 1.6,
                  ),
                ),
                const SizedBox(height: 56),

                // Feature pills
                const Wrap(
                  spacing: 12,
                  runSpacing: 12,
                  alignment: WrapAlignment.center,
                  children: [
                    _FeaturePill(icon: Icons.bolt, label: 'Real-time sync'),
                    _FeaturePill(icon: Icons.table_chart, label: 'Auto Sheets'),
                    _FeaturePill(icon: Icons.smart_toy, label: 'AI extraction'),
                    _FeaturePill(icon: Icons.filter_alt, label: 'Eligibility filter'),
                  ],
                ),
                const SizedBox(height: 56),

                // CTA Button
                SizedBox(
                  width: double.infinity,
                  child: ElevatedButton.icon(
                    onPressed: () => context.go('/connect'),
                    icon: const Icon(Icons.wallet, size: 22),
                    label: const Text('Connect WhatsApp'),
                  ),
                ),
                const SizedBox(height: 16),

                const Text(
                  'No messages are sent. Read-only access only.',
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    fontSize: 12,
                    color: AppTheme.textSecondary,
                  ),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _FeaturePill extends StatelessWidget {
  final IconData icon;
  final String label;
  const _FeaturePill({required this.icon, required this.label});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
      decoration: BoxDecoration(
        color: AppTheme.surfaceVariant,
        borderRadius: BorderRadius.circular(20),
        border: Border.all(color: AppTheme.border),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, size: 14, color: AppTheme.primary),
          const SizedBox(width: 6),
          Text(
            label,
            style: const TextStyle(
              fontSize: 13,
              color: AppTheme.textPrimary,
              fontWeight: FontWeight.w500,
            ),
          ),
        ],
      ),
    );
  }
}