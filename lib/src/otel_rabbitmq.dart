// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry_api/dartastic_opentelemetry_api.dart';

import 'otel_rabbitmq_suppression.dart';

const _tracerName = 'dartastic_otel_libs_rabbitmq';

APITracer _tracer() => OTelAPI.tracerProvider().getTracer(_tracerName);

/// Wraps a RabbitMQ publish in a PRODUCER span using OTel messaging
/// semconv (messaging.system=rabbitmq).
Future<T> tracedRabbitmqPublish<T>({
  required String exchange,
  required String routingKey,
  required Future<T> Function() invoke,
  int? payloadSize,
  String? messageId,
  bool? mandatory,
}) async {
  if (rabbitmqInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': 'rabbitmq',
    'messaging.operation': 'publish',
    'messaging.destination.name': exchange,
    'messaging.rabbitmq.routing_key': routingKey,
  };
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (messageId != null) m['messaging.message.id'] = messageId;
  if (mandatory != null) m['messaging.rabbitmq.mandatory'] = mandatory;

  final span = _tracer().startSpan(
    'publish $exchange',
    kind: SpanKind.producer,
    attributes: OTelAPI.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTelAPI.attributes([
      OTelAPI.attributeString(
        ErrorAttributes.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps the user's handling of one RabbitMQ delivery in a CONSUMER
/// span. Call inside the consumer callback before `basicAck`.
Future<T> tracedRabbitmqConsume<T>({
  required String queue,
  required Future<T> Function() invoke,
  String? consumerTag,
  int? deliveryTag,
  bool? redelivered,
  String? messageId,
}) async {
  if (rabbitmqInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': 'rabbitmq',
    'messaging.operation': 'receive',
    'messaging.destination.name': queue,
  };
  if (consumerTag != null) {
    m['messaging.rabbitmq.consumer_tag'] = consumerTag;
  }
  if (deliveryTag != null) {
    m['messaging.rabbitmq.delivery_tag'] = deliveryTag;
  }
  if (redelivered != null) {
    m['messaging.rabbitmq.redelivered'] = redelivered;
  }
  if (messageId != null) m['messaging.message.id'] = messageId;

  final span = _tracer().startSpan(
    'receive $queue',
    kind: SpanKind.consumer,
    attributes: OTelAPI.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTelAPI.attributes([
      OTelAPI.attributeString(
        ErrorAttributes.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
