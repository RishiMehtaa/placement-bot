import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';

import '../core/theme.dart';
import '../services/api_service.dart';

class ChatImportScreen extends StatefulWidget {
  const ChatImportScreen({super.key});

  @override
  State<ChatImportScreen> createState() => _ChatImportScreenState();
}

class _ChatImportScreenState extends State<ChatImportScreen> {
  PlatformFile? _selectedFile;
  bool _isImporting = false;
  String? _errorMessage;
  Map<String, dynamic>? _importResult;

  Future<void> _pickTxtFile() async {
    final result = await FilePicker.platform.pickFiles(
      type: FileType.custom,
      allowedExtensions: ['txt'],
      withData: true,
    );

    if (result == null || result.files.isEmpty) {
      return;
    }

    final file = result.files.single;
    if (file.bytes == null || file.bytes!.isEmpty) {
      setState(() {
        _errorMessage = 'Selected file has no readable data. Please pick another .txt file.';
      });
      return;
    }

    setState(() {
      _selectedFile = file;
      _errorMessage = null;
      _importResult = null;
    });
  }

  Future<void> _startImport() async {
    final file = _selectedFile;
    if (file == null || file.bytes == null || file.bytes!.isEmpty) {
      setState(() {
        _errorMessage = 'Please choose a .txt chat export first.';
      });
      return;
    }

    setState(() {
      _isImporting = true;
      _errorMessage = null;
      _importResult = null;
    });

    try {
      final response = await ApiService().importChatExportTxt(
        fileBytes: file.bytes!,
        fileName: file.name,
      );

      if (!mounted) return;
      setState(() {
        _importResult = response;
      });
    } catch (e) {
      if (!mounted) return;
      setState(() {
        _errorMessage = e.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _isImporting = false;
        });
      }
    }
  }

  int _asInt(Map<String, dynamic>? map, String key) {
    final value = map?[key];
    if (value is int) return value;
    if (value is num) return value.toInt();
    if (value is String) return int.tryParse(value) ?? 0;
    return 0;
  }

  @override
  Widget build(BuildContext context) {
    final filename = _selectedFile?.name;

    return Scaffold(
      appBar: AppBar(
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () => context.go('/dashboard'),
        ),
        title: const Text('Import Chat Export'),
      ),
      body: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 900),
          child: ListView(
            padding: const EdgeInsets.all(20),
            children: [
              Container(
                padding: const EdgeInsets.all(20),
                decoration: BoxDecoration(
                  color: AppTheme.surface,
                  borderRadius: BorderRadius.circular(12),
                  border: Border.all(color: AppTheme.border),
                ),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    const Text(
                      'Upload WhatsApp Export (.txt)',
                      style: TextStyle(
                        color: AppTheme.textPrimary,
                        fontSize: 18,
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                    const SizedBox(height: 8),
                    const Text(
                      'The system will parse chat messages and synchronously run extraction for each message.',
                      style: TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 13,
                      ),
                    ),
                    const SizedBox(height: 18),
                    Row(
                      children: [
                        OutlinedButton.icon(
                          onPressed: _isImporting ? null : _pickTxtFile,
                          icon: const Icon(Icons.upload_file_rounded),
                          label: const Text('Choose .txt File'),
                        ),
                        const SizedBox(width: 12),
                        Expanded(
                          child: Text(
                            filename ?? 'No file selected',
                            overflow: TextOverflow.ellipsis,
                            style: const TextStyle(
                              color: AppTheme.textSecondary,
                              fontSize: 13,
                            ),
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 16),
                    ElevatedButton.icon(
                      onPressed: _isImporting ? null : _startImport,
                      icon: _isImporting
                          ? const SizedBox(
                              width: 16,
                              height: 16,
                              child: CircularProgressIndicator(strokeWidth: 2),
                            )
                          : const Icon(Icons.play_arrow_rounded),
                      label: Text(_isImporting ? 'Importing...' : 'Start Import & Extraction'),
                    ),
                  ],
                ),
              ),
              if (_errorMessage != null) ...[
                const SizedBox(height: 16),
                Container(
                  padding: const EdgeInsets.all(14),
                  decoration: BoxDecoration(
                    color: AppTheme.error.withValues(alpha: 0.2),
                    borderRadius: BorderRadius.circular(10),
                    border: Border.all(color: AppTheme.error),
                  ),
                  child: Text(
                    _errorMessage!,
                    style: const TextStyle(color: AppTheme.textPrimary),
                  ),
                ),
              ],
              if (_importResult != null) ...[
                const SizedBox(height: 16),
                Container(
                  padding: const EdgeInsets.all(20),
                  decoration: BoxDecoration(
                    color: AppTheme.surface,
                    borderRadius: BorderRadius.circular(12),
                    border: Border.all(color: AppTheme.border),
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text(
                        'Import Summary',
                        style: TextStyle(
                          color: AppTheme.textPrimary,
                          fontSize: 18,
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                      const SizedBox(height: 12),
                      Wrap(
                        spacing: 12,
                        runSpacing: 12,
                        children: [
                          _SummaryChip(label: 'Parsed Messages', value: _asInt(_importResult, 'parsed_messages')),
                          _SummaryChip(label: 'Accepted', value: _asInt(_importResult, 'accepted')),
                          _SummaryChip(label: 'Skipped', value: _asInt(_importResult, 'skipped')),
                          _SummaryChip(label: 'Processed', value: _asInt(_importResult, 'processed_success')),
                          _SummaryChip(label: 'Failed', value: _asInt(_importResult, 'processed_failed')),
                        ],
                      ),
                    ],
                  ),
                ),
              ],
            ],
          ),
        ),
      ),
    );
  }
}

class _SummaryChip extends StatelessWidget {
  final String label;
  final int value;

  const _SummaryChip({required this.label, required this.value});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 10),
      decoration: BoxDecoration(
        color: AppTheme.primary.withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(10),
        border: Border.all(color: AppTheme.border),
      ),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            '$value',
            style: const TextStyle(
              color: AppTheme.textPrimary,
              fontWeight: FontWeight.w700,
              fontSize: 18,
            ),
          ),
          const SizedBox(height: 2),
          Text(
            label,
            style: const TextStyle(
              color: AppTheme.textSecondary,
              fontSize: 12,
            ),
          ),
        ],
      ),
    );
  }
}
