import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'core/router.dart';
import 'core/theme.dart';


void main() {
  runApp(
    const ProviderScope(
      child: PlacementBotApp(),
    ),
  );
}

class PlacementBotApp extends StatelessWidget {
  const PlacementBotApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp.router(
      title: 'Placement Intelligence',
      debugShowCheckedModeBanner: false,
      theme: AppTheme.dark,
      routerConfig: appRouter,
    );
  }
}