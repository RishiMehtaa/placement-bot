import 'package:dio/dio.dart';
import '../core/constants.dart';
import '../models/opportunity.dart';

class ApiService {
  static final ApiService _instance = ApiService._internal();
  factory ApiService() => _instance;
  ApiService._internal();

  final Dio _dio = Dio(
    BaseOptions(
      baseUrl: AppConstants.baseUrl,
      connectTimeout: const Duration(seconds: 10),
      receiveTimeout: const Duration(seconds: 15),
      headers: {'Content-Type': 'application/json'},
    ),
  );

  Future<OpportunitiesPage> getOpportunities({
    int page = 1,
    int pageSize = 20,
    String? search,
  }) async {
    try {
      final queryParams = <String, dynamic>{
        'page': page,
        'page_size': pageSize,
        if (search != null && search.isNotEmpty) 'search': search,
      };
      final response = await _dio.get(
        AppConstants.opportunitiesEndpoint,
        queryParameters: queryParams,
      );
      return OpportunitiesPage.fromJson(response.data as Map<String, dynamic>);
    } on DioException catch (e) {
      throw _handleError(e);
    }
  }

  Future<Map<String, dynamic>> getOpportunityDetail(String id) async {
    try {
      final response = await _dio.get('${AppConstants.opportunitiesEndpoint}/$id');
      return response.data as Map<String, dynamic>;
    } on DioException catch (e) {
      throw _handleError(e);
    }
  }

  Future<void> updateOpportunityApplied({
    required String opportunityId,
    required String role,
    required bool applied,
  }) async {
    try {
      await _dio.patch(
        '${AppConstants.opportunitiesEndpoint}/$opportunityId/applied',
        data: {
          'role': role,
          'applied': applied,
        },
      );
    } on DioException catch (e) {
      throw _handleError(e);
    }
  }

  Future<AnalyticsSummary> getAnalyticsSummary() async {
    try {
      final response = await _dio.get(AppConstants.analyticsEndpoint);
      return AnalyticsSummary.fromJson(response.data as Map<String, dynamic>);
    } on DioException catch (e) {
      throw _handleError(e);
    }
  }

  Future<List<Map<String, dynamic>>> getTimeline() async {
    try {
      final response = await _dio.get(AppConstants.timelineEndpoint);
      final data = response.data as Map<String, dynamic>;
      return (data['timeline'] as List).cast<Map<String, dynamic>>();
    } on DioException catch (e) {
      throw _handleError(e);
    }
  }

  Future<Map<String, dynamic>> getDemoGroups() async {
    try {
      final response = await _dio.get(AppConstants.demoGroupsEndpoint);
      return response.data as Map<String, dynamic>;
    } on DioException catch (e) {
      throw _handleError(e);
    }
  }

  String _handleError(DioException e) {
    if (e.type == DioExceptionType.connectionTimeout ||
        e.type == DioExceptionType.receiveTimeout) {
      return 'Connection timed out. Check your network.';
    }
    if (e.type == DioExceptionType.connectionError) {
      return 'Cannot reach server. Is EC2 running?';
    }
    return e.message ?? 'Unknown error occurred.';
  }
}