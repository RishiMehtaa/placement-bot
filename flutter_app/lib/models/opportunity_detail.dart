class ContributingMessage {
  final String messageId;
  final String text;
  final String? timestamp;
  final String? sender;
  final String? contributionRole;

  const ContributingMessage({
    required this.messageId,
    required this.text,
    this.timestamp,
    this.sender,
    this.contributionRole,
  });

  factory ContributingMessage.fromJson(Map<String, dynamic> json) {
    return ContributingMessage(
      messageId: json['message_id'] as String,
      text: json['text'] as String,
      timestamp: json['timestamp'] as String?,
      sender: json['sender'] as String?,
      contributionRole: json['contribution_role'] as String?,
    );
  }
}

class OpportunityDetail {
  final String id;
  final String? company;
  final String? role;
  final String? duration;
  final String? internalFormLink;
  final String? startDate;
  final String? location;
  final String? eligible;
  final String? eligibleReason;
  final String? deadline;
  final String? package;
  final String? jdLink;
  final List<String> notes;
  final double? confidence;
  final String? createdAt;
  final String? updatedAt;
  final String? syncStatus;
  final String? lastSyncedAt;
  final List<ContributingMessage> messages;

  const OpportunityDetail({
    required this.id,
    this.company,
    this.role,
    this.duration,
    this.internalFormLink,
    this.startDate,
    this.location,
    this.eligible,
    this.eligibleReason,
    this.deadline,
    this.package,
    this.jdLink,
    required this.notes,
    this.confidence,
    this.createdAt,
    this.updatedAt,
    this.syncStatus,
    this.lastSyncedAt,
    required this.messages,
  });

  factory OpportunityDetail.fromJson(Map<String, dynamic> json) {
    return OpportunityDetail(
      id: json['id'] as String,
      company: json['company'] as String?,
      role: json['role'] as String?,
      duration: json['duration'] as String?,
      internalFormLink: json['internal_form_link'] as String?,
      startDate: json['start_date'] as String?,
      location: json['location'] as String?,
      eligible: json['eligible'] as String?,
      eligibleReason: json['eligible_reason'] as String?,
      deadline: json['deadline'] as String?,
      package: json['package'] as String?,
      jdLink: json['jd_link'] as String?,
      notes: (json['notes'] as List?)?.map((e) => e.toString()).toList() ?? [],
      confidence: (json['confidence'] as num?)?.toDouble(),
      createdAt: json['created_at'] as String?,
      updatedAt: json['updated_at'] as String?,
      syncStatus: json['sync_status'] as String?,
      lastSyncedAt: json['last_synced_at'] as String?,
      messages: (json['messages'] as List?)
              ?.map((e) => ContributingMessage.fromJson(e as Map<String, dynamic>))
              .toList() ??
          [],
    );
  }

  DateTime? get deadlineDate {
    if (deadline == null) return null;
    try {
      return DateTime.parse(deadline!);
    } catch (_) {
      return null;
    }
  }

  Duration? get timeUntilDeadline {
    final d = deadlineDate;
    if (d == null) return null;
    return d.difference(DateTime.now());
  }

  bool get isDeadlinePast {
    final d = deadlineDate;
    if (d == null) return false;
    return d.isBefore(DateTime.now());
  }

  bool get isDeadlineSoon {
    final remaining = timeUntilDeadline;
    if (remaining == null) return false;
    return remaining.inDays <= 3 && remaining.isNegative == false;
  }
}