import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import '../core/theme.dart';
import '../core/constants.dart';
import 'package:dio/dio.dart';

class GroupSelectionScreen extends StatefulWidget {
  const GroupSelectionScreen({super.key});

  @override
  State<GroupSelectionScreen> createState() => _GroupSelectionScreenState();
}

class _GroupSelectionScreenState extends State<GroupSelectionScreen> {
  List<Map<String, String>> _groups = [];
  String? _selectedId;
  bool _loading = true;
  String? _error;

  @override
  void initState() {
    super.initState();
    _loadGroups();
  }

  Future<void> _loadGroups() async {
    try {
      final dio = Dio();
      final response = await dio.get(
        '${AppConstants.baseUrl}${AppConstants.demoGroupsEndpoint}',
      );
      final data = response.data as Map<String, dynamic>;
      final groups = (data['groups'] as List).cast<Map<String, dynamic>>();
      setState(() {
        _groups = groups
            .map((g) => {
                  'id': g['id'].toString(),
                  'name': g['name'].toString(),
                })
            .toList();
        _loading = false;
      });
    } catch (_) {
      // Fallback to mock data if API unreachable
      setState(() {
        _groups = [
          {'id': '120363406687081890@g.us', 'name': 'DJ Sanghvi Placements 2027'},
          {'id': 'demo-group-2', 'name': 'CE Internships'},
          {'id': 'demo-group-3', 'name': 'Placement Updates'},
        ];
        _loading = false;
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Select Group'),
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () => context.go('/connect'),
        ),
      ),
      body: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 480),
          child: Padding(
            padding: const EdgeInsets.all(32),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Your WhatsApp Groups',
                  style: TextStyle(
                    fontSize: 22,
                    fontWeight: FontWeight.w700,
                    color: AppTheme.textPrimary,
                  ),
                ),
                const SizedBox(height: 8),
                const Text(
                  'Select the placement group you want to track.',
                  style: TextStyle(
                    fontSize: 14,
                    color: AppTheme.textSecondary,
                  ),
                ),
                const SizedBox(height: 32),

                if (_loading)
                  const Center(child: CircularProgressIndicator(color: AppTheme.primary))
                else if (_error != null)
                  Center(
                    child: Text(_error!, style: const TextStyle(color: AppTheme.error)),
                  )
                else
                  Expanded(
                    child: ListView.separated(
                      itemCount: _groups.length,
                      separatorBuilder: (_, __) => const SizedBox(height: 12),
                      itemBuilder: (context, index) {
                        final group = _groups[index];
                        final isSelected = _selectedId == group['id'];
                        return GestureDetector(
                          onTap: () => setState(() => _selectedId = group['id']),
                          child: AnimatedContainer(
                            duration: const Duration(milliseconds: 200),
                            padding: const EdgeInsets.all(18),
                            decoration: BoxDecoration(
                              color: isSelected
                                  ? AppTheme.primary
                                  : AppTheme.surface,
                              borderRadius: BorderRadius.circular(12),
                              border: Border.all(
                                color: isSelected ? AppTheme.primary : AppTheme.border,
                                width: isSelected ? 2 : 1,
                              ),
                            ),
                            child: Row(
                              children: [
                                Container(
                                  width: 44,
                                  height: 44,
                                  decoration: BoxDecoration(
                                    color: AppTheme.primary,
                                    borderRadius: BorderRadius.circular(10),
                                  ),
                                  child: const Icon(
                                    Icons.groups_rounded,
                                    color: AppTheme.primary,
                                    size: 22,
                                  ),
                                ),
                                const SizedBox(width: 16),
                                Expanded(
                                  child: Text(
                                    group['name']!,
                                    style: const TextStyle(
                                      fontSize: 15,
                                      fontWeight: FontWeight.w600,
                                      color: AppTheme.textPrimary,
                                    ),
                                  ),
                                ),
                                if (isSelected)
                                  const Icon(
                                    Icons.check_circle,
                                    color: AppTheme.primary,
                                    size: 22,
                                  ),
                              ],
                            ),
                          ),
                        );
                      },
                    ),
                  ),

                const SizedBox(height: 24),
                SizedBox(
                  width: double.infinity,
                  child: ElevatedButton(
                    onPressed: _selectedId == null ? null : () => context.go('/dashboard'),
                    style: ElevatedButton.styleFrom(
                      disabledBackgroundColor: AppTheme.surfaceVariant,
                      disabledForegroundColor: AppTheme.textSecondary,
                    ),
                    child: const Text('Start Tracking'),
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