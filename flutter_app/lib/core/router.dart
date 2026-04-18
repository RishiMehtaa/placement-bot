import 'package:go_router/go_router.dart';
import 'package:flutter/material.dart';
import '../screens/welcome_screen.dart';
import '../screens/connect_whatsapp_screen.dart';
import '../screens/group_selection_screen.dart';
import '../screens/dashboard_screen.dart';
import '../screens/opportunity_detail_screen.dart';
import '../screens/analytics_screen.dart';
import '../screens/chat_import_screen.dart';

final appRouter = GoRouter(
  initialLocation: '/',
  routes: [
    GoRoute(
      path: '/',
      builder: (context, state) => const WelcomeScreen(),
    ),
    GoRoute(
      path: '/connect',
      builder: (context, state) => const ConnectWhatsAppScreen(),
    ),
    GoRoute(
      path: '/groups',
      builder: (context, state) => const GroupSelectionScreen(),
    ),
    GoRoute(
      path: '/dashboard',
      builder: (context, state) => const DashboardScreen(),
    ),
    GoRoute(
      path: '/opportunity/:id',
      builder: (context, state) {
        final id = state.pathParameters['id']!;
        return OpportunityDetailScreen(familyId: id);
      },
    ),
    GoRoute(
      path: '/analytics',
      builder: (context, state) => const AnalyticsScreen(),
    ),
//     GoRoute(
//   path: '/opportunity/:id',
//   builder: (context, state) {
//     final id = state.pathParameters['id']!;
//     return OpportunityDetailScreen(familyId: id);
//   },
  
// ),
// GoRoute(
//   path: '/analytics',
//   builder: (context, state) => const AnalyticsScreen(),
// ),
GoRoute(
      path: '/import',
      builder: (context, state) => const ChatImportScreen(),
    ),
  ],
  errorBuilder: (context, state) => Scaffold(
    body: Center(
      child: Text('Page not found: ${state.error}'),
    ),
  ),
);