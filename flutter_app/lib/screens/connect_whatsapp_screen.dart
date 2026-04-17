import 'package:flutter/material.dart';
import 'package:go_router/go_router.dart';
import 'package:super_icons/super_icons.dart';
import '../core/theme.dart';

class ConnectWhatsAppScreen extends StatefulWidget {
  const ConnectWhatsAppScreen({super.key});

  @override
  State<ConnectWhatsAppScreen> createState() => _ConnectWhatsAppScreenState();
}

class _ConnectWhatsAppScreenState extends State<ConnectWhatsAppScreen> {
  bool _scanning = false;

  void _simulateScan() async {
    setState(() => _scanning = true);
    await Future.delayed(const Duration(seconds: 2));
    if (mounted) context.go('/groups');
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Connect WhatsApp'),
        leading: IconButton(
          icon: const Icon(Icons.arrow_back),
          onPressed: () => context.go('/'),
        ),
      ),
      body: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 440),
          child: Padding(
            padding: const EdgeInsets.all(32),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                const Text(
                  'Scan QR Code',
                  style: TextStyle(
                    fontSize: 24,
                    fontWeight: FontWeight.w700,
                    color: AppTheme.textPrimary,
                  ),
                ),
                const SizedBox(height: 8),
                const Text(
                  'Open WhatsApp → Linked Devices → Link a Device',
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    fontSize: 14,
                    color: AppTheme.textSecondary,
                  ),
                ),
                const SizedBox(height: 40),

                // QR Code box
                Container(
                  width: 240,
                  height: 240,
                  decoration: BoxDecoration(
                    color: Colors.white,
                    borderRadius: BorderRadius.circular(16),
                    border: Border.all(
                      color: AppTheme.primary,
                      width: 3,
                    ),
                    boxShadow: const [
                      BoxShadow(
                        color: AppTheme.primary,
                        blurRadius: 24,
                        spreadRadius: 2,
                      ),
                    ],
                  ),
                  child: _scanning
                      ? const Center(
                          child: CircularProgressIndicator(
                            color: AppTheme.primary,
                          ),
                        )
                      : Stack(
                          alignment: Alignment.center,
                          children: [
                            // QR grid simulation
                            CustomPaint(
                              size: const Size(200,200),
                              painter: _QrPainter(),
                            ),
                            Container(
                              width: 48,
                              height: 48,
                              decoration: BoxDecoration(
                                color: AppTheme.primary,
                                borderRadius: BorderRadius.circular(10),
                              ),
                              child: const Icon(
                                SuperIcons.bs_whatsapp,
                                color: Colors.white,
                                size: 28,
                              ),
                            ),
                          ],
                        ),
                ),
                const SizedBox(height: 40),

                if (!_scanning) ...[
                  SizedBox(
                    width: double.infinity,
                    child: ElevatedButton.icon(
                      onPressed: _simulateScan,
                      icon: const Icon(Icons.qr_code_scanner, size: 20),
                      label: const Text('QR Scanned — Continue'),
                    ),
                  ),
                  const SizedBox(height: 12),
                  TextButton(
                    onPressed: () => context.go('/dashboard'),
                    child: const Text(
                      'Already connected — Go to dashboard',
                      style: TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 13,
                      ),
                    ),
                  ),
                ] else ...[
                  const Text(
                    'Connecting...',
                    style: TextStyle(
                      color: AppTheme.primary,
                      fontSize: 15,
                      fontWeight: FontWeight.w500,
                    ),
                  ),
                ],

                const SizedBox(height: 32),
                Container(
                  padding: const EdgeInsets.all(16),
                  decoration: BoxDecoration(
                    color: AppTheme.surfaceVariant,
                    borderRadius: BorderRadius.circular(12),
                    border: Border.all(color: AppTheme.border),
                  ),
                  child: const Row(
                    children: [
                      Icon(Icons.lock_outline, size: 16, color: AppTheme.primary),
                      SizedBox(width: 10),
                      Expanded(
                        child: Text(
                          'This app only reads messages. It never sends or modifies anything on WhatsApp.',
                          style: TextStyle(
                            fontSize: 12,
                            color: AppTheme.textSecondary,
                            height: 1.5,
                          ),
                        ),
                      ),
                    ],
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

class _QrPainter extends CustomPainter {
  @override
  void paint(Canvas canvas, Size size) {
    final paint = Paint()
      ..color = Colors.black
      ..style = PaintingStyle.fill;

    const cellSize = 10.0;
    const cols = 25;
    const rows = 25;
    // final offsetX = (size.width - cols * cellSize) / 2;
    // final offsetY = (size.height - rows * cellSize) / 2;
    final offsetX = -5;
    final offsetY = -5;


    // Simple pattern to simulate QR code
    final pattern = [
      [1,1,1,1,1,1,1,0,1,0,1,0,1,0,1,1,1,1,1,1,1],
      [1,0,0,0,0,0,1,0,1,1,0,1,0,0,1,0,0,0,0,0,1],
      [1,0,1,1,1,0,1,0,1,0,1,1,1,0,1,0,1,1,1,0,1],
      [1,0,1,1,1,0,1,0,0,1,0,1,0,0,1,0,1,1,1,0,1],
      [1,0,1,1,1,0,1,0,1,0,1,1,1,0,1,0,1,1,1,0,1],
      [1,0,0,0,0,0,1,0,0,1,0,1,0,0,1,0,0,0,0,0,1],
      [1,1,1,1,1,1,1,0,1,0,1,0,1,0,1,1,1,1,1,1,1],
      [0,0,0,0,0,0,0,0,1,1,0,1,0,0,0,0,0,0,0,0,0],
      [1,0,1,1,0,1,1,1,0,1,1,0,1,1,1,0,1,1,0,1,1],
      [0,1,0,0,1,0,0,0,1,0,0,1,0,0,0,1,0,0,1,0,0],
      [1,0,1,0,1,0,1,1,0,1,1,0,1,1,1,0,1,0,1,0,1],
      [0,1,0,1,0,1,0,0,1,0,0,1,0,0,0,1,0,1,0,1,0],
      [1,0,1,0,1,0,1,1,0,1,1,0,1,1,1,0,1,0,1,0,1],
      [0,0,0,0,0,0,0,0,1,0,0,1,0,0,0,0,0,0,0,0,0],
      [1,1,1,1,1,1,1,0,0,1,0,0,1,0,1,1,1,1,1,1,1],
      [1,0,0,0,0,0,1,0,1,0,1,1,0,0,1,0,0,0,0,0,1],
      [1,0,1,1,1,0,1,1,0,1,0,0,1,0,1,0,1,1,1,0,1],
      [1,0,1,1,1,0,1,0,1,0,1,1,0,0,1,0,1,1,1,0,1],
      [1,0,1,1,1,0,1,0,0,1,0,0,1,0,1,0,1,1,1,0,1],
      [1,0,0,0,0,0,1,0,1,0,1,1,0,0,1,0,0,0,0,0,1],
      [1,1,1,1,1,1,1,0,0,1,0,0,1,0,1,1,1,1,1,1,1],
    ];

    for (int row = 0; row < pattern.length; row++) {
      for (int col = 0; col < pattern[row].length; col++) {
        if (pattern[row][col] == 1) {
          canvas.drawRect(
            Rect.fromLTWH(
              offsetX + col * cellSize,
              offsetY + row * cellSize,
              cellSize - 1,
              cellSize - 1,
            ),
            paint,
          );
        }
      }
    }
  }

  @override
  bool shouldRepaint(covariant CustomPainter oldDelegate) => false;
}